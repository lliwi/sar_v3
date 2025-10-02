import ldap3
from ldap3.utils.conv import escape_filter_chars
from flask import current_app
from app.models import ADGroup, User, Role
from app import db
from app.utils.db_utils import commit_with_retry, retry_on_deadlock
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class LDAPService:
    def __init__(self):
        self.host = current_app.config.get('LDAP_HOST')
        self.base_dn = current_app.config.get('LDAP_BASE_DN')
        self.group_dn = current_app.config.get('LDAP_GROUP_DN')
        self.bind_user_dn = current_app.config.get('LDAP_BIND_USER_DN')
        self.bind_user_password = current_app.config.get('LDAP_BIND_USER_PASSWORD')
        
        # LDAP attribute mappings
        self.attr_user = current_app.config.get('LDAP_ATTR_USER', 'cn')
        self.attr_department = current_app.config.get('LDAP_ATTR_DEPARTMENT', 'department')
        self.attr_email = current_app.config.get('LDAP_ATTR_EMAIL', 'mail')
        self.attr_firstname = current_app.config.get('LDAP_ATTR_FIRSTNAME', 'givenName')
        self.attr_lastname = current_app.config.get('LDAP_ATTR_LASTNAME', 'sn')
        
        # Multiple OU search configuration
        self.search_ous = current_app.config.get('LDAP_SEARCH_OUS', [])
    
    def get_connection(self, user_dn=None, password=None):
        """Get LDAP connection"""
        try:
            server = ldap3.Server(self.host, get_info=ldap3.ALL)
            
            if user_dn and password:
                # User authentication
                conn = ldap3.Connection(server, user=user_dn, password=password, auto_bind=True)
            else:
                # Service account authentication
                conn = ldap3.Connection(
                    server, 
                    user=self.bind_user_dn, 
                    password=self.bind_user_password, 
                    auto_bind=True
                )
            
            return conn
        except Exception as e:
            logger.error(f"Error connecting to LDAP: {str(e)}")
            # Send admin notification for LDAP connection errors
            try:
                from app.services.email_service import send_admin_error_notification
                send_admin_error_notification(
                    error_type="LDAP_CONNECTION_FAILED",
                    service_name="LDAP",
                    error_message=f"Failed to connect to LDAP server {self.host}: {str(e)}"
                )
            except:
                pass
            return None
    
    def _search_in_multiple_ous(self, conn, search_filter, attributes, scope=ldap3.SUBTREE):
        """Search for objects in multiple OUs if configured, otherwise search in base DN"""
        all_entries = []
        
        if self.search_ous:
            # Search in each configured OU with pagination
            for ou in self.search_ous:
                ou = ou.strip()  # Remove any whitespace
                if ou:
                    try:
                        logger.debug(f"Searching in OU: {ou}")
                        ou_entries = self._search_with_pagination(conn, ou, search_filter, attributes)
                        all_entries.extend(ou_entries)
                        logger.debug(f"Found {len(ou_entries)} entries in {ou}")
                    except Exception as e:
                        logger.warning(f"Error searching in OU {ou}: {str(e)}")
                        continue
        else:
            # Fallback to base DN search with pagination
            logger.debug(f"Searching in base DN: {self.base_dn}")
            all_entries = self._search_with_pagination(conn, self.base_dn, search_filter, attributes)
        
        # Return all found entries (cannot modify conn.entries directly)
        return all_entries
    
    def _search_with_pagination(self, conn, search_base, search_filter, attributes, page_size=1000):
        """Search with pagination to get all results"""
        all_entries = []
        
        try:
            # Initialize paged search
            conn.search(
                search_base=search_base,
                search_filter=search_filter,
                attributes=attributes,
                paged_size=page_size,
                search_scope=ldap3.SUBTREE
            )
            
            # Get first page
            all_entries.extend(conn.entries)
            logger.debug(f"First page: {len(conn.entries)} entries from {search_base}")
            
            # Get additional pages if available
            cookie = conn.result['controls']['1.2.840.113556.1.4.319']['value']['cookie']
            while cookie:
                conn.search(
                    search_base=search_base,
                    search_filter=search_filter,
                    attributes=attributes,
                    paged_size=page_size,
                    search_scope=ldap3.SUBTREE,
                    paged_cookie=cookie
                )
                
                all_entries.extend(conn.entries)
                logger.debug(f"Additional page: {len(conn.entries)} entries from {search_base}")
                
                try:
                    cookie = conn.result['controls']['1.2.840.113556.1.4.319']['value']['cookie']
                except KeyError:
                    # No more pages
                    break
                    
            logger.info(f"Pagination completed: {len(all_entries)} total entries from {search_base}")
            return all_entries
            
        except Exception as e:
            logger.error(f"Error in paginated search for {search_base}: {str(e)}")
            return all_entries  # Return what we have so far
    
    def authenticate_user(self, username, password):
        """Authenticate user against LDAP"""
        try:
            # First, search for the user in the entire domain to find their DN
            conn = self.get_connection()
            if not conn:
                return None

            # Search for user by sAMAccountName or cn across all OUs
            # Escape username to prevent LDAP injection
            safe_username = escape_filter_chars(username)
            search_filter = f"(&(objectClass=user)(|(sAMAccountName={safe_username})({self.attr_user}={safe_username})(userPrincipalName={safe_username}@*)))"
            attributes = [
                'cn', 'distinguishedName', 'sAMAccountName', 'displayName', 'memberOf', 'userPrincipalName',
                self.attr_email, self.attr_department, self.attr_firstname, self.attr_lastname, self.attr_user
            ]
            
            # Use multi-OU search if configured, otherwise search base DN
            entries = self._search_in_multiple_ous(conn, search_filter, attributes, ldap3.SUBTREE)
            
            user_entry = None
            user_dn = None
            
            if entries:
                # Found user, get their DN
                user_entry = entries[0]
                user_dn = user_entry.entry_dn
                
                # Log which OU the user was found in
                if self.search_ous:
                    for ou in self.search_ous:
                        if ou.strip() in user_dn:
                            logger.info(f"User {username} found in OU: {ou.strip()}")
                            break
                else:
                    logger.info(f"User {username} found in base DN search")
                
                conn.unbind()
                
                # Now try to authenticate with the found DN
                auth_conn = self.get_connection(user_dn, password)
                if not auth_conn:
                    logger.warning(f"Authentication failed for user {username} with DN {user_dn}")
                    return None
                
                # Authentication successful, prepare user data
                # Extract individual attributes using configurable mappings
                email_attr = getattr(user_entry, self.attr_email, None)
                department_attr = getattr(user_entry, self.attr_department, None)
                firstname_attr = getattr(user_entry, self.attr_firstname, None)
                lastname_attr = getattr(user_entry, self.attr_lastname, None)
                
                # Build full name from first and last name if available
                full_name = ""
                if firstname_attr and lastname_attr:
                    full_name = f"{str(firstname_attr)} {str(lastname_attr)}"
                elif user_entry.displayName:
                    full_name = str(user_entry.displayName)
                else:
                    full_name = str(user_entry.cn)
                
                user_data = {
                    'username': str(user_entry.sAMAccountName) if user_entry.sAMAccountName else username,
                    'full_name': full_name,
                    'email': str(email_attr) if email_attr else f"{username}@example.org",
                    'department': str(department_attr) if department_attr else None,
                    'first_name': str(firstname_attr) if firstname_attr else None,
                    'last_name': str(lastname_attr) if lastname_attr else None,
                    'groups': [str(group) for group in user_entry.memberOf] if user_entry.memberOf else [],
                    'distinguished_name': user_dn
                }
                
                auth_conn.unbind()
                logger.info(f"Successfully authenticated user {username} from DN {user_dn}")
                return user_data
            else:
                conn.unbind()
                logger.warning(f"User {username} not found in LDAP")
                return None
            
        except Exception as e:
            logger.error(f"Error authenticating user {username}: {str(e)}")
            return None
    
    def get_user_details(self, username):
        """Get user details from LDAP without authentication"""
        try:
            # Search for the user in the entire domain to find their details
            conn = self.get_connection()
            if not conn:
                return None

            # Search for user by sAMAccountName or cn across all OUs
            # Escape username to prevent LDAP injection
            safe_username = escape_filter_chars(username)
            search_filter = f"(&(objectClass=user)(|(sAMAccountName={safe_username})({self.attr_user}={safe_username})(userPrincipalName={safe_username}@*)))"
            attributes = [
                'cn', 'distinguishedName', 'sAMAccountName', 'displayName', 'memberOf', 'userPrincipalName',
                self.attr_email, self.attr_department, self.attr_firstname, self.attr_lastname, self.attr_user,
                'userAccountControl'  # Include to detect disabled accounts
            ]

            # Use multi-OU search if configured, otherwise search base DN
            entries = self._search_in_multiple_ous(conn, search_filter, attributes, ldap3.SUBTREE)

            if entries:
                user_entry = entries[0]

                # Extract individual attributes using configurable mappings
                email_attr = getattr(user_entry, self.attr_email, None)
                department_attr = getattr(user_entry, self.attr_department, None)
                firstname_attr = getattr(user_entry, self.attr_firstname, None)
                lastname_attr = getattr(user_entry, self.attr_lastname, None)

                # Build full name from first and last name if available
                full_name = ""
                if firstname_attr and lastname_attr:
                    full_name = f"{str(firstname_attr)} {str(lastname_attr)}"
                elif user_entry.displayName:
                    full_name = str(user_entry.displayName)
                else:
                    full_name = str(user_entry.cn)

                # Extract email and other details
                email = str(email_attr) if email_attr else f"{username}@example.org"
                department = str(department_attr) if department_attr else None
                distinguished_name = str(user_entry.distinguishedName)
                sam_account = str(user_entry.sAMAccountName) if user_entry.sAMAccountName else username

                # Check if user is disabled
                is_disabled = self._is_user_disabled(user_entry)

                conn.unbind()

                return {
                    'username': sam_account.lower(),
                    'full_name': full_name,
                    'email': email,
                    'department': department,
                    'distinguished_name': distinguished_name,
                    'is_disabled': is_disabled
                }
            else:
                logger.warning(f"User {username} not found in LDAP")
                conn.unbind()
                return None

        except Exception as e:
            logger.error(f"Error getting user details for {username}: {str(e)}")
            return None

    def get_multiple_groups_members_batch(self, group_dns, batch_size=10):
        """
        Get members of multiple groups in optimized batches

        Args:
            group_dns: List of group distinguished names
            batch_size: Number of groups to process in each batch

        Returns:
            dict: {group_dn: [member_dns]}
        """
        all_memberships = {}

        try:
            # Process in batches to avoid timeouts and memory issues
            for i in range(0, len(group_dns), batch_size):
                batch = group_dns[i:i + batch_size]
                logger.debug(f"Processing group batch {i//batch_size + 1}: {len(batch)} groups")

                for group_dn in batch:
                    try:
                        members = self.get_group_members(group_dn)
                        all_memberships[group_dn] = members
                        logger.debug(f"Group {group_dn}: {len(members)} members")
                    except Exception as e:
                        logger.error(f"Error getting members for {group_dn}: {e}")
                        all_memberships[group_dn] = []

            logger.info(f"Batch processing completed: {len(group_dns)} groups processed")
            return all_memberships

        except Exception as e:
            logger.error(f"Error in batch group processing: {str(e)}")
            return all_memberships

    def get_user_details_with_cache(self, username, failed_cache=None):
        """
        Optimized version of get_user_details with failed user caching

        Args:
            username: Username to lookup
            failed_cache: Set of usernames known to have failed lookups

        Returns:
            dict or None: User details or None if not found
        """
        try:
            # Check failed cache first
            if failed_cache and username in failed_cache:
                logger.debug(f"👻 Skipping known failed user: {username} (cached)")
                return None

            user_details = self.get_user_details(username)

            if not user_details and failed_cache is not None:
                # Add to failed cache
                failed_cache.add(username)
                logger.debug(f"Added {username} to failed cache")

            return user_details

        except Exception as e:
            # Add to failed cache on error
            if failed_cache is not None:
                failed_cache.add(username)
            logger.error(f"Error in cached user lookup for {username}: {str(e)}")
            raise e

    def get_multiple_users_details_batch(self, usernames, batch_size=20):
        """
        Get details for multiple users in optimized batches

        Args:
            usernames: List of usernames to lookup
            batch_size: Number of users to process in each batch

        Returns:
            dict: {username: user_details}
        """
        all_user_details = {}
        failed_cache = set()

        try:
            # Process in batches to avoid LDAP timeouts
            for i in range(0, len(usernames), batch_size):
                batch = usernames[i:i + batch_size]
                logger.debug(f"Processing user batch {i//batch_size + 1}: {len(batch)} users")

                for username in batch:
                    try:
                        user_details = self.get_user_details_with_cache(username, failed_cache)
                        if user_details:
                            all_user_details[username] = user_details
                        else:
                            logger.debug(f"User not found: {username}")
                    except Exception as e:
                        logger.error(f"Error getting details for user {username}: {e}")
                        continue

            logger.info(f"Batch user processing completed: {len(all_user_details)} users found, {len(failed_cache)} failed")
            return all_user_details

        except Exception as e:
            logger.error(f"Error in batch user processing: {str(e)}")
            return all_user_details

    def extract_username_from_dn(self, member_dn):
        """
        Extract username from Distinguished Name with robust parsing

        Args:
            member_dn: Distinguished Name string

        Returns:
            str or None: Extracted username or None if invalid
        """
        try:
            if not member_dn:
                return None

            member_dn_lower = member_dn.lower()

            # Skip known non-user objects
            if any(skip_pattern in member_dn_lower for skip_pattern in [
                'ou=devices', 'ou=computers', 'cn=protected users',
                'foreignsecurityprincipals', 's-1-5-'
            ]):
                return None

            # Robust CN extraction - handle various DN formats
            if 'cn=' in member_dn_lower:
                cn_parts = member_dn_lower.split('cn=')
                if len(cn_parts) > 1:
                    # Get the first CN= part (typically the user)
                    cn_value = cn_parts[1].split(',')[0].strip()
                    if cn_value and not any(x in cn_value for x in ['users', 'builtin', 'system']):
                        return cn_value
            elif 'uid=' in member_dn_lower:
                uid_parts = member_dn_lower.split('uid=')
                if len(uid_parts) > 1:
                    return uid_parts[1].split(',')[0].strip()

            return None

        except (IndexError, AttributeError):
            return None

    def get_unique_groups_from_active_permissions(self):
        """
        Get unique AD groups from all active folder permissions

        Returns:
            list: List of unique group distinguished names
        """
        try:
            from app.models import Folder

            unique_groups = set()
            active_folders = Folder.query.filter_by(is_active=True).all()

            for folder in active_folders:
                for permission in folder.permissions:
                    if permission.is_active:
                        unique_groups.add(permission.ad_group.distinguished_name)

            return list(unique_groups)

        except Exception as e:
            logger.error(f"Error getting unique groups from permissions: {str(e)}")
            return []

    def get_user_groups(self, username):
        """Get groups for a specific user"""
        try:
            conn = self.get_connection()
            if not conn:
                return []

            # Search for user by sAMAccountName or configured user attribute across all OUs
            # Escape username to prevent LDAP injection
            safe_username = escape_filter_chars(username)
            search_filter = f"(&(objectClass=user)(|(sAMAccountName={safe_username})({self.attr_user}={safe_username})))"
            attributes = ['memberOf']
            
            # Use multi-OU search if configured, otherwise search base DN
            entries = self._search_in_multiple_ous(conn, search_filter, attributes, ldap3.SUBTREE)
            
            groups = []
            if entries:
                user_entry = entries[0]
                if user_entry.memberOf:
                    groups = [str(group) for group in user_entry.memberOf]
            
            conn.unbind()
            return groups
            
        except Exception as e:
            logger.error(f"Error getting groups for user {username}: {str(e)}")
            return []
    
    def sync_groups(self):
        """Sync AD groups to database"""
        try:
            conn = self.get_connection()
            if not conn:
                raise Exception("No se pudo conectar a LDAP")
            
            # Search for all security groups using multi-OU search if configured,
            # otherwise fallback to group_dn or base_dn
            search_filter = "(objectClass=group)"
            attributes = ['cn', 'distinguishedName', 'description', 'groupType']
            
            if self.search_ous:
                # Use multi-OU search for groups
                all_entries = self._search_in_multiple_ous(conn, search_filter, attributes, ldap3.SUBTREE)
                # Also include groups from base DN to capture system groups
                if self.group_dn:
                    # Use pagination to get all groups
                    group_entries = self._search_with_pagination(conn, self.group_dn, search_filter, attributes)
                    # Avoid duplicates by checking if entries are already in all_entries
                    existing_dns = {entry.entry_dn for entry in all_entries}
                    for entry in group_entries:
                        if entry.entry_dn not in existing_dns:
                            all_entries.append(entry)
            else:
                # Fallback to original behavior with pagination
                search_base = self.group_dn if self.group_dn else self.base_dn
                all_entries = self._search_with_pagination(conn, search_base, search_filter, attributes)
            
            synced_count = 0
            current_time = datetime.utcnow()
            batch_size = 100  # Process in batches of 100 groups
            batch_count = 0
            
            logger.info(f"Starting group sync: {len(all_entries)} groups to process")
            
            for i, entry in enumerate(all_entries):
                group_name = str(entry.cn)
                distinguished_name = str(entry.distinguishedName)
                description = str(entry.description) if entry.description else None
                group_type = str(entry.groupType) if entry.groupType else 'Security'
                
                # Check if group exists in database (first by DN, then by name to avoid conflicts)
                ad_group = ADGroup.query.filter_by(distinguished_name=distinguished_name).first()
                if not ad_group:
                    # Check if a group with same name exists (different DN)
                    ad_group = ADGroup.query.filter_by(name=group_name).first()
                
                if ad_group:
                    # Update existing group
                    ad_group.name = group_name
                    ad_group.distinguished_name = distinguished_name  # Update DN if it changed
                    ad_group.description = description
                    ad_group.group_type = group_type
                    ad_group.last_sync = current_time
                    ad_group.mark_ad_active()  # This sets is_active=True AND ad_status='active'
                else:
                    # Create new group only if it doesn't exist by name or DN
                    ad_group = ADGroup(
                        name=group_name,
                        distinguished_name=distinguished_name,
                        description=description,
                        group_type=group_type,
                        last_sync=current_time,
                        is_active=True
                    )
                    db.session.add(ad_group)
                
                synced_count += 1
                batch_count += 1
                
                # Commit in batches to avoid long transactions
                if batch_count >= batch_size or i == len(all_entries) - 1:
                    if commit_with_retry(max_attempts=3):
                        logger.debug(f"Groups batch {(i//batch_size)+1} committed: {batch_count} groups")
                        batch_count = 0
                    else:
                        logger.error(f"Failed to commit groups batch after retries")
                        batch_count = 0
            
            # Mark groups not found in LDAP as inactive (separate transaction)
            try:
                old_groups = ADGroup.query.filter(
                    ADGroup.last_sync < current_time,
                    ADGroup.is_active == True
                ).all()
                
                inactive_count = 0
                for group in old_groups:
                    group.mark_ad_not_found()  # This marks as not_found AND inactive
                    inactive_count += 1

                    if inactive_count % batch_size == 0:
                        commit_with_retry(max_attempts=3)
                        logger.debug(f"Marked {inactive_count} groups as inactive")

                commit_with_retry(max_attempts=3)
                logger.info(f"Marked {len(old_groups)} old groups as inactive")
                
            except Exception as e:
                logger.error(f"Error marking old groups as inactive: {str(e)}")
                db.session.rollback()
            conn.unbind()
            
            logger.info(f"AD Groups sync completed. {synced_count} groups processed.")
            return synced_count
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error syncing AD groups: {str(e)}")
            # Send admin notification for sync errors
            try:
                from app.services.email_service import send_admin_error_notification
                send_admin_error_notification(
                    error_type="AD_SYNC_FAILED",
                    service_name="LDAP",
                    error_message=f"Failed to sync AD groups: {str(e)}"
                )
            except:
                pass
            raise e
    
    def sync_single_group(self, group_dn):
        """Sync a specific AD group by its Distinguished Name"""
        try:
            conn = self.get_connection()
            if not conn:
                logger.error("Could not connect to LDAP")
                return False
            
            # Search for the specific group
            # Escape group_dn to prevent LDAP injection
            safe_group_dn = escape_filter_chars(group_dn)
            search_filter = f"(distinguishedName={safe_group_dn})"
            attributes = ['cn', 'distinguishedName', 'description', 'groupType']
            
            conn.search(
                search_base=group_dn,
                search_filter='(objectClass=group)',
                search_scope=ldap3.BASE,
                attributes=attributes
            )
            
            if not conn.entries:
                logger.warning(f"Group not found in AD: {group_dn}")
                return False
            
            entry = conn.entries[0]
            group_name = str(entry.cn)
            distinguished_name = str(entry.distinguishedName)
            description = str(entry.description) if entry.description else None
            group_type = str(entry.groupType) if entry.groupType else 'Security'
            
            current_time = datetime.utcnow()
            
            # Find existing group in database
            ad_group = ADGroup.query.filter_by(distinguished_name=distinguished_name).first()
            
            if ad_group:
                # Update existing group
                ad_group.name = group_name
                ad_group.distinguished_name = distinguished_name
                ad_group.description = description
                ad_group.group_type = group_type
                ad_group.last_sync = current_time
                ad_group.is_active = True
                logger.info(f"Updated existing AD group: {group_name}")
            else:
                # Create new group
                ad_group = ADGroup(
                    name=group_name,
                    distinguished_name=distinguished_name,
                    description=description,
                    group_type=group_type,
                    is_active=True,
                    last_sync=current_time
                )
                db.session.add(ad_group)
                logger.info(f"Created new AD group: {group_name}")
            
            db.session.commit()
            conn.unbind()
            
            logger.info(f"Single group sync completed for: {group_name}")
            return True
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error syncing single AD group {group_dn}: {str(e)}")
            return False
    
    def get_group_members(self, group_dn):
        """Get members of a specific group"""
        try:
            conn = self.get_connection()
            if not conn:
                return []

            # Escape group_dn to prevent LDAP injection
            safe_group_dn = escape_filter_chars(group_dn)
            search_filter = f"(distinguishedName={safe_group_dn})"
            attributes = ['member']
            
            # Use multi-OU search if configured, otherwise use group_dn or base_dn
            if self.search_ous:
                all_entries = self._search_in_multiple_ous(conn, search_filter, attributes, ldap3.SUBTREE)
                # Also search in base DN to capture system groups
                if self.group_dn:
                    conn.search(
                        search_base=self.group_dn,
                        search_filter=search_filter,
                        attributes=attributes
                    )
                    # Avoid duplicates by checking if entries are already in all_entries
                    existing_dns = {entry.entry_dn for entry in all_entries}
                    for entry in conn.entries:
                        if entry.entry_dn not in existing_dns:
                            all_entries.append(entry)
            else:
                # Fallback to original behavior
                search_base = self.group_dn if self.group_dn else self.base_dn
                conn.search(
                    search_base=search_base,
                    search_filter=search_filter,
                    attributes=attributes
                )
                all_entries = conn.entries
            
            members = []
            if all_entries:
                group_entry = all_entries[0]
                if group_entry.member:
                    members = [str(member) for member in group_entry.member]
            
            conn.unbind()
            return members
            
        except Exception as e:
            logger.error(f"Error getting group members for {group_dn}: {str(e)}")
            return []
    
    def verify_group_exists(self, group_name):
        """Verify if a group exists in AD"""
        try:
            conn = self.get_connection()
            if not conn:
                return False

            # Escape group_name to prevent LDAP injection
            safe_group_name = escape_filter_chars(group_name)
            search_filter = f"(&(objectClass=group)(cn={safe_group_name}))"
            attributes = ['cn']
            
            # Use multi-OU search if configured, otherwise use group_dn or base_dn
            if self.search_ous:
                all_entries = self._search_in_multiple_ous(conn, search_filter, attributes, ldap3.SUBTREE)
                # Also search in base DN to capture system groups
                if self.group_dn:
                    conn.search(
                        search_base=self.group_dn,
                        search_filter=search_filter,
                        attributes=attributes
                    )
                    # Avoid duplicates by checking if entries are already in all_entries
                    existing_dns = {entry.entry_dn for entry in all_entries}
                    for entry in conn.entries:
                        if entry.entry_dn not in existing_dns:
                            all_entries.append(entry)
            else:
                # Fallback to original behavior
                search_base = self.group_dn if self.group_dn else self.base_dn
                conn.search(
                    search_base=search_base,
                    search_filter=search_filter,
                    attributes=attributes
                )
                all_entries = conn.entries
            
            exists = len(all_entries) > 0
            conn.unbind()
            return exists

        except Exception as e:
            logger.error(f"Error verifying group {group_name}: {str(e)}")
            return False

    def _is_user_disabled(self, entry):
        """Check if user account is disabled in AD"""
        try:
            if hasattr(entry, 'userAccountControl'):
                # Extract value from LDAP3 Attribute object
                uac_value = entry.userAccountControl.value if hasattr(entry.userAccountControl, 'value') else entry.userAccountControl
                uac = int(uac_value)
                # Bit 2 (0x0002) = ACCOUNTDISABLE
                is_disabled = (uac & 0x0002) != 0
                if is_disabled:
                    username = str(entry.sAMAccountName) if hasattr(entry, 'sAMAccountName') else 'unknown'
                    logger.info(f"🔒 User {username} detected as DISABLED (UAC={uac})")
                return is_disabled
        except (ValueError, TypeError, AttributeError) as e:
            username = str(entry.sAMAccountName) if hasattr(entry, 'sAMAccountName') else 'unknown'
            logger.warning(f"Could not parse userAccountControl for {username}: {str(e)}")
        return False

    def sync_users(self):
        """Sync AD users to database"""
        try:
            conn = self.get_connection()
            if not conn:
                raise Exception("No se pudo conectar a LDAP")
            
            # Search for all users using multi-OU search if configured,
            # otherwise fallback to base_dn
            # Include ALL users (active and disabled) to properly detect status
            search_filter = "(objectClass=user)"
            attributes = [
                'cn', 'distinguishedName', 'sAMAccountName', 'displayName', 'memberOf', 'userPrincipalName',
                self.attr_email, self.attr_department, self.attr_firstname, self.attr_lastname, self.attr_user,
                'userAccountControl'
            ]
            
            # Use multi-OU search if configured, otherwise search base DN
            if self.search_ous:
                all_entries = self._search_in_multiple_ous(conn, search_filter, attributes, ldap3.SUBTREE)
            else:
                # Fallback to base DN search with pagination
                all_entries = self._search_with_pagination(conn, self.base_dn, search_filter, attributes)
            
            synced_count = 0
            current_time = datetime.utcnow()
            batch_size = 50  # Process in smaller batches for users (more complex data)
            batch_count = 0
            
            logger.info(f"Starting user sync: {len(all_entries)} users to process")
            
            # Get or create default user role
            user_role = Role.query.filter_by(name='user').first()
            if not user_role:
                user_role = Role(name='user', description='Usuario estándar del sistema')
                db.session.add(user_role)
                db.session.flush()
            
            for i, entry in enumerate(all_entries):
                try:
                    # Extract user information
                    username = str(entry.sAMAccountName) if entry.sAMAccountName else None
                    if not username:
                        continue  # Skip users without sAMAccountName
                    
                    username = username.lower()  # Normalize username
                    
                    # Extract individual attributes using configurable mappings
                    email_attr = getattr(entry, self.attr_email, None)
                    department_attr = getattr(entry, self.attr_department, None)
                    firstname_attr = getattr(entry, self.attr_firstname, None)
                    lastname_attr = getattr(entry, self.attr_lastname, None)
                    
                    # Build full name from first and last name if available
                    full_name = ""
                    if firstname_attr and lastname_attr:
                        full_name = f"{str(firstname_attr)} {str(lastname_attr)}"
                    elif entry.displayName:
                        full_name = str(entry.displayName)
                    else:
                        full_name = str(entry.cn)
                    
                    # Extract email
                    email = str(email_attr) if email_attr else f"{username}@example.org"
                    department = str(department_attr) if department_attr else None
                    distinguished_name = str(entry.distinguishedName)
                    
                    # Check if user exists in database
                    user = User.query.filter_by(username=username).first()
                    
                    if user:
                        # Update existing user
                        user.full_name = full_name
                        user.email = email
                        user.department = department
                        user.distinguished_name = distinguished_name
                        user.last_sync = current_time

                        # Check if user is disabled in AD
                        if self._is_user_disabled(entry):
                            user.mark_ad_disabled()
                        else:
                            user.mark_ad_active()  # This sets is_active=True AND ad_status='active'
                    else:
                        # Create new user
                        user = User(
                            username=username,
                            full_name=full_name,
                            email=email,
                            department=department,
                            distinguished_name=distinguished_name,
                            is_active=True,
                            last_sync=current_time
                        )
                        
                        # Assign default role
                        user.roles.append(user_role)
                        db.session.add(user)
                    
                    synced_count += 1
                    batch_count += 1
                    
                    # Commit in batches to avoid long transactions
                    if batch_count >= batch_size or i == len(all_entries) - 1:
                        if commit_with_retry(max_attempts=3):
                            logger.debug(f"Users batch {(i//batch_size)+1} committed: {batch_count} users")
                            batch_count = 0
                        else:
                            logger.error(f"Failed to commit users batch after retries")
                            batch_count = 0
                    
                except Exception as e:
                    logger.warning(f"Error processing user entry: {str(e)}")
                    continue
            
            # Mark users not found in LDAP as inactive (separate transaction)
            try:
                old_users = User.query.filter(
                    User.last_sync < current_time,
                    User.is_active == True,
                    User.distinguished_name.isnot(None)  # Only users that came from LDAP
                ).all()
                
                inactive_count = 0
                for user in old_users:
                    user.mark_ad_not_found()  # This marks as not_found AND inactive
                    inactive_count += 1

                    if inactive_count % batch_size == 0:
                        commit_with_retry(max_attempts=3)
                        logger.debug(f"Marked {inactive_count} users as inactive")

                commit_with_retry(max_attempts=3)
                logger.info(f"Marked {len(old_users)} old users as inactive")
                
            except Exception as e:
                logger.error(f"Error marking old users as inactive: {str(e)}")
                db.session.rollback()
            conn.unbind()
            
            logger.info(f"AD Users sync completed. {synced_count} users processed.")
            return synced_count
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error syncing AD users: {str(e)}")
            raise e
    
    def validate_folder_permissions(self, folder_id=None):
        """
        Validate folder permissions against AD.
        Detects discrepancies between database and actual AD permissions.
        
        Args:
            folder_id: Specific folder to validate (None for all folders)
            
        Returns:
            dict: Validation results with discrepancies found
        """
        try:
            from app.models import Folder, FolderPermission
            
            results = {
                'success': True,
                'validated_folders': 0,
                'discrepancies': [],
                'warnings': [],
                'summary': {}
            }
            
            # Get folders to validate
            if folder_id:
                folders = Folder.query.filter_by(id=folder_id, is_active=True).all()
            else:
                folders = Folder.query.filter_by(is_active=True).all()
            
            conn = self.get_connection()
            if not conn:
                results['success'] = False
                results['warnings'].append('Cannot connect to LDAP server')
                return results
            
            for folder in folders:
                folder_result = self._validate_single_folder(conn, folder)
                results['validated_folders'] += 1
                
                if folder_result['discrepancies']:
                    results['discrepancies'].extend(folder_result['discrepancies'])
                
                if folder_result['warnings']:
                    results['warnings'].extend(folder_result['warnings'])
            
            conn.unbind()
            
            # Generate summary
            results['summary'] = {
                'total_discrepancies': len(results['discrepancies']),
                'missing_in_ad': len([d for d in results['discrepancies'] if d['type'] == 'missing_in_ad']),
                'extra_in_ad': len([d for d in results['discrepancies'] if d['type'] == 'extra_in_ad']),
                'group_not_exists': len([d for d in results['discrepancies'] if d['type'] == 'group_not_exists'])
            }
            
            logger.info(f"Folder permissions validation completed. {results['validated_folders']} folders validated, {results['summary']['total_discrepancies']} discrepancies found.")
            return results
            
        except Exception as e:
            logger.error(f"Error validating folder permissions: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'validated_folders': 0,
                'discrepancies': [],
                'warnings': [],
                'summary': {}
            }
    
    def _validate_single_folder(self, conn, folder):
        """
        Validate permissions for a single folder against AD.
        
        Args:
            conn: LDAP connection
            folder: Folder model instance
            
        Returns:
            dict: Validation results for the folder
        """
        result = {
            'folder_id': folder.id,
            'folder_path': folder.path,
            'discrepancies': [],
            'warnings': []
        }
        
        try:
            from app.models import FolderPermission
            
            # Get database permissions for this folder
            db_permissions = FolderPermission.query.filter_by(
                folder_id=folder.id, 
                is_active=True
            ).all()
            
            for permission in db_permissions:
                # Check if AD group exists
                if not self.verify_group_exists(permission.ad_group.name):
                    result['discrepancies'].append({
                        'type': 'group_not_exists',
                        'folder_id': folder.id,
                        'folder_path': folder.path,
                        'ad_group': permission.ad_group.name,
                        'permission_type': permission.permission_type,
                        'message': f'AD Group "{permission.ad_group.name}" does not exist in Active Directory'
                    })
                    continue
                
                # Check if group has actual permissions on the folder path
                actual_permissions = self._get_folder_permissions_from_ad(conn, folder.path, permission.ad_group.name)
                expected_permission = permission.permission_type
                
                if not self._has_permission_in_ad(actual_permissions, expected_permission):
                    result['discrepancies'].append({
                        'type': 'missing_in_ad',
                        'folder_id': folder.id,
                        'folder_path': folder.path,
                        'ad_group': permission.ad_group.name,
                        'permission_type': permission.permission_type,
                        'message': f'Database shows {permission.permission_type} permission for "{permission.ad_group.name}" but not found in AD'
                    })
            
            return result
            
        except Exception as e:
            result['warnings'].append(f'Error validating folder {folder.path}: {str(e)}')
            logger.error(f"Error validating folder {folder.path}: {str(e)}")
            return result
    
    def _get_folder_permissions_from_ad(self, conn, folder_path, group_name):
        """
        Get actual permissions for a folder from AD (placeholder implementation).
        
        Note: This method would need to be implemented based on your specific
        AD structure and how permissions are stored/managed.
        
        Args:
            conn: LDAP connection
            folder_path: Path to the folder
            group_name: AD group name
            
        Returns:
            list: List of permissions found in AD
        """
        # TODO: Implement actual AD permission checking
        # This is a placeholder - you would implement the actual logic
        # to check folder permissions in your AD environment
        
        logger.warning(f"AD permission checking not implemented for {folder_path} - {group_name}")
        return []
    
    def _has_permission_in_ad(self, actual_permissions, expected_permission):
        """
        Check if expected permission exists in actual AD permissions.
        
        Args:
            actual_permissions: List of permissions from AD
            expected_permission: Expected permission type ('read' or 'write')
            
        Returns:
            bool: True if permission exists in AD
        """
        # TODO: Implement based on your AD permission structure
        # This is a placeholder implementation
        
        # For now, assume all permissions exist (to avoid false positives)
        # until actual AD permission checking is implemented
        return True
    
    def validate_user_groups(self, user_id=None):
        """
        Validate user group memberships against AD.
        
        Args:
            user_id: Specific user to validate (None for all users)
            
        Returns:
            dict: Validation results with discrepancies found
        """
        try:
            from app.models import User, UserFolderPermission
            
            results = {
                'success': True,
                'validated_users': 0,
                'discrepancies': [],
                'warnings': [],
                'summary': {}
            }
            
            # Get users to validate
            if user_id:
                users = User.query.filter_by(id=user_id, is_active=True).all()
            else:
                users = User.query.filter(
                    User.is_active == True,
                    User.distinguished_name.isnot(None)  # Only LDAP users
                ).all()
            
            conn = self.get_connection()
            if not conn:
                results['success'] = False
                results['warnings'].append('Cannot connect to LDAP server')
                return results
            
            for user in users:
                user_result = self._validate_single_user(conn, user)
                results['validated_users'] += 1
                
                if user_result['discrepancies']:
                    results['discrepancies'].extend(user_result['discrepancies'])
                
                if user_result['warnings']:
                    results['warnings'].extend(user_result['warnings'])
            
            conn.unbind()
            
            # Generate summary
            results['summary'] = {
                'total_discrepancies': len(results['discrepancies']),
                'user_not_in_group': len([d for d in results['discrepancies'] if d['type'] == 'user_not_in_group']),
                'user_in_unexpected_group': len([d for d in results['discrepancies'] if d['type'] == 'user_in_unexpected_group'])
            }
            
            logger.info(f"User groups validation completed. {results['validated_users']} users validated, {results['summary']['total_discrepancies']} discrepancies found.")
            return results
            
        except Exception as e:
            logger.error(f"Error validating user groups: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'validated_users': 0,
                'discrepancies': [],
                'warnings': [],
                'summary': {}
            }
    
    def _validate_single_user(self, conn, user):
        """
        Validate group memberships for a single user against AD.
        
        Args:
            conn: LDAP connection
            user: User model instance
            
        Returns:
            dict: Validation results for the user
        """
        result = {
            'user_id': user.id,
            'username': user.username,
            'discrepancies': [],
            'warnings': []
        }
        
        try:
            from app.models import UserFolderPermission
            
            # Get user's expected group memberships from database
            user_permissions = UserFolderPermission.query.filter_by(
                user_id=user.id,
                is_active=True
            ).all()
            
            # Get user's actual group memberships from AD
            actual_groups = self.get_user_groups(user.username)
            
            # Check each expected permission
            for permission in user_permissions:
                # For direct user permissions, we would need to check
                # if the user actually has the permission in AD
                # This is a placeholder for the actual implementation
                pass
            
            return result
            
        except Exception as e:
            result['warnings'].append(f'Error validating user {user.username}: {str(e)}')
            logger.error(f"Error validating user {user.username}: {str(e)}")
            return result