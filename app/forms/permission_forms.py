from flask_wtf import FlaskForm
from wtforms import SelectField, TextAreaField, SubmitField, DateTimeField, IntegerField
from wtforms.validators import DataRequired, Length, Optional, ValidationError
from app.models import Folder, ADGroup

class PermissionRequestForm(FlaskForm):
    folder_id = IntegerField('Carpeta', validators=[
        DataRequired(message='Debe seleccionar una carpeta')
    ])
    validator_id = IntegerField('Validador', validators=[
        DataRequired(message='Debe seleccionar un validador')
    ])
    permission_type = SelectField('Tipo de Permiso', choices=[
        ('read', 'Lectura'),
        ('write', 'Escritura')
    ], validators=[DataRequired(message='Debe seleccionar el tipo de permiso')])
    business_need = TextAreaField('Describa la necesidad que justifica este permiso', validators=[
        DataRequired(message='La descripción de la necesidad es requerida'),
        Length(min=10, max=1000, message='La descripción debe tener entre 10 y 1000 caracteres')
    ])
    submit = SubmitField('Solicitar Permiso')
    
    def __init__(self, *args, **kwargs):
        super(PermissionRequestForm, self).__init__(*args, **kwargs)
        # Both folder_id and validator_id are now IntegerFields - no choices needed
        # But we still need to provide folders for the template rendering
        self._folders = Folder.query.filter_by(is_active=True).order_by(Folder.path).all()
    
    @property
    def folders(self):
        """Get folders for template rendering"""
        if not hasattr(self, '_folders'):
            self._folders = Folder.query.filter_by(is_active=True).order_by(Folder.path).all()
        return self._folders
    
    def validate_folder_id(self, field):
        """Custom validation for folder_id to ensure folder exists and is active"""
        if not field.data:
            raise ValidationError('Debe seleccionar una carpeta')
        
        # Ensure the field data is an integer
        try:
            folder_id = int(field.data)
        except (ValueError, TypeError):
            raise ValidationError('ID de carpeta inválido')
        
        folder = Folder.query.get(folder_id)
        if not folder:
            raise ValidationError('La carpeta seleccionada no existe')
        
        if not folder.is_active:
            raise ValidationError('La carpeta seleccionada no está activa')
    
    def validate_validator_id(self, field):
        """Custom validation for validator_id that dynamically checks against folder validators"""
        if not field.data:
            raise ValidationError('Debe seleccionar un validador')
        
        # Ensure the field data is an integer
        try:
            validator_id = int(field.data)
        except (ValueError, TypeError):
            raise ValidationError('ID de validador inválido')
        
        # Get the selected folder (folder validation should run first)
        folder = None
        if self.folder_id.data:
            try:
                folder_id = int(self.folder_id.data)
                folder = Folder.query.get(folder_id)
            except (ValueError, TypeError):
                pass
        
        if not folder:
            raise ValidationError('Debe seleccionar una carpeta válida primero')
        
        # Get authorized validators for this folder
        from app.models import User
        authorized_validator_ids = set()
        
        # Add folder owners
        for owner in folder.owners:
            authorized_validator_ids.add(owner.id)
        
        # Add folder validators
        for validator in folder.validators:
            authorized_validator_ids.add(validator.id)
        
        # Check if selected validator is authorized for this folder
        if validator_id not in authorized_validator_ids:
            raise ValidationError('El validador seleccionado no está autorizado para esta carpeta')

class PermissionValidationForm(FlaskForm):
    action = SelectField('Acción', choices=[
        ('approve', 'Aprobar'),
        ('reject', 'Rechazar')
    ], validators=[DataRequired(message='Debe seleccionar una acción')])
    validator_id = SelectField('Validador', coerce=lambda x: int(x) if x else None, validators=[
        DataRequired(message='Debe seleccionar un validador')
    ])
    validation_comment = TextAreaField('Comentario (Opcional)', validators=[
        Optional(),  # Hace el comentario opcional
        Length(max=500, message='El comentario no puede exceder 500 caracteres')
    ])
    submit = SubmitField('Procesar Solicitud')
    
    def __init__(self, folder=None, *args, **kwargs):
        super(PermissionValidationForm, self).__init__(*args, **kwargs)
        if folder:
            # Obtener usuarios autorizados para validar esta carpeta
            from app.models import User
            authorized_validators = []
            
            # Administradores pueden validar cualquier solicitud
            admins = User.query.join(User.roles).filter_by(name='Administrador').all()
            for admin in admins:
                authorized_validators.append((admin.id, f"{admin.full_name} (Administrador)"))
            
            # Propietarios de la carpeta
            for owner in folder.owners:
                if owner not in [admin for admin in admins]:  # Evitar duplicados
                    authorized_validators.append((owner.id, f"{owner.full_name} (Propietario)"))
            
            # Validadores específicos de la carpeta
            for validator in folder.validators:
                if validator not in [admin for admin in admins] and validator not in folder.owners:
                    authorized_validators.append((validator.id, f"{validator.full_name} (Validador)"))
            
            # Ordenar por nombre
            authorized_validators.sort(key=lambda x: x[1])
            
            self.validator_id.choices = [('', '-- Seleccione un validador --')] + authorized_validators