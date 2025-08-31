# SAR - Sistema de Gestión de Permisos de Carpetas

![Python](https://img.shields.io/badge/python-v3.11+-blue.svg)
![Flask](https://img.shields.io/badge/flask-v2.3+-green.svg)
![Docker](https://img.shields.io/badge/docker-ready-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

SAR (Sistema de Accesos y Recursos) es una aplicación web desarrollada en Flask para gestionar los permisos de acceso a las carpetas de una empresa de manera centralizada. Proporciona una fuente única de verdad para los permisos, facilitando su administración, auditoría y solicitud.

## 🚀 Características Principales

- **Autenticación LDAP/Active Directory** - Integración completa con AD para autenticación y sincronización
- **Gestión de Permisos** - Solicitud, aprobación y seguimiento de permisos de carpetas
- **Workflow de Aprobación** - Sistema de validación por propietarios y validadores autorizados
- **Notificaciones Automáticas** - Emails automáticos para solicitudes y cambios de estado
- **Validación por Email** - Enlaces seguros para aprobar/rechazar desde el correo
- **Integración con Airflow** - Aplicación automática de cambios en Active Directory
- **Auditoría Completa** - Registro detallado de todas las acciones del sistema
- **API REST** - Endpoints para integraciones y consultas externas
- **Interfaz Responsiva** - Dashboard moderno con Bootstrap 5
- **Seguridad Avanzada** - Protección CSRF, headers de seguridad, control de acceso por roles

## 🏗️ Arquitectura

### Tecnologías Utilizadas

- **Backend**: Flask (Python)
- **Base de Datos**: PostgreSQL/MySQL
- **Cache/Broker**: Redis
- **Worker**: Celery (para tareas asíncronas)
- **Contenedores**: Docker & Docker Compose
- **IA**: Ollama (modelos LLM locales)
- **Orquestación**: Apache Airflow

### Extensiones de Flask

- `Flask-SQLAlchemy` - ORM para base de datos
- `Flask-Migrate` - Migraciones de base de datos
- `Flask-Login` - Gestión de sesiones
- `Flask-WTF` - Formularios seguros
- `Flask-LDAP3-Login` - Autenticación LDAP
- `Flask-Talisman` - Headers de seguridad
- `Flask-SeaSurf` - Protección CSRF

## 📋 Prerrequisitos

- Docker & Docker Compose
- Acceso a un servidor Active Directory/LDAP
- Servidor SMTP para notificaciones
- (Opcional) Apache Airflow para automatización

## 🔧 Instalación y Configuración

### 1. Clonar el Repositorio

```bash
git clone <repository-url>
cd SAR_v3
```

### 2. Configurar Variables de Entorno

```bash
cp .env.example .env
```

Editar el archivo `.env` con tus configuraciones:

```bash
# Flask Configuration
FLASK_ENV=production
SECRET_KEY=tu-clave-secreta-muy-segura

# Database Configuration
DATABASE_URL=postgresql://saruser:password@db:5432/sarapp
POSTGRES_DB=sarapp
POSTGRES_USER=saruser
POSTGRES_PASSWORD=tu-password-seguro

# LDAP Configuration
LDAP_HOST=ldap://tu-controlador-dominio.com
LDAP_BASE_DN=dc=empresa,dc=com
LDAP_USER_DN=ou=Users,dc=empresa,dc=com
LDAP_GROUP_DN=ou=Groups,dc=empresa,dc=com
LDAP_BIND_USER_DN=cn=cuenta-servicio,ou=Users,dc=empresa,dc=com
LDAP_BIND_USER_PASSWORD=password-cuenta-servicio

# SMTP Configuration
SMTP_SERVER=smtp.empresa.com
SMTP_PORT=587
SMTP_USERNAME=noreply@empresa.com
SMTP_PASSWORD=password-smtp
SMTP_USE_TLS=true

# Celery Configuration
CELERY_BROKER_URL=redis://redis:6379/0
CELERY_RESULT_BACKEND=redis://redis:6379/0

# Airflow Configuration (Opcional)
AIRFLOW_API_URL=http://airflow:8080/api/v1
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=password-airflow

# AI Configuration (Opcional)
OLLAMA_BASE_URL=http://ollama:11434
```

### 3. Ejecutar con Docker Compose

```bash
# Construir y ejecutar todos los servicios
docker-compose up -d

# Ver logs
docker-compose logs -f web
```

### 4. Inicializar Base de Datos

```bash
# Ejecutar script de inicialización
docker-compose exec web python init_db.py

# O usar migraciones
docker-compose exec web flask db upgrade
```

### 5. Verificar Instalación

Abrir navegador en `http://localhost:5000` y usar credenciales de Active Directory.

## 🐳 Servicios Docker

El `docker-compose.yml` incluye los siguientes servicios:

- **web** - Aplicación Flask principal (puerto 5000)
- **db** - Base de datos PostgreSQL (puerto 5432)
- **redis** - Cache y broker para Celery (puerto 6379)
- **celery** - Worker para tareas asíncronas

## 📊 Modelo de Datos

### Entidades Principales

- **User** - Usuarios del sistema con roles
- **Role** - Roles del sistema (Administrador, Owner, Validador, Usuario)
- **Folder** - Inventario de carpetas con propietarios y validadores
- **ADGroup** - Grupos de seguridad de Active Directory
- **FolderPermission** - Relación carpeta-grupo con tipo de permiso
- **PermissionRequest** - Solicitudes de permisos con workflow
- **AuditEvent** - Registro de auditoría de todas las acciones

### Diagrama de Relaciones

```
User ←→ Role (many-to-many)
User ←→ Folder (owners, validators - many-to-many)
User → PermissionRequest (requester, validator)
Folder → PermissionRequest
ADGroup → PermissionRequest
Folder ←→ ADGroup (through FolderPermission)
User → AuditEvent
```

## 🔄 Flujo de Trabajo

### Solicitud de Permisos

1. **Usuario solicita permiso** → Sistema registra solicitud
2. **Notificación automática** → Email a propietarios/validadores
3. **Validación** → Aprobación/rechazo por web o email
4. **Aplicación** → Generación de archivo para Airflow
5. **Ejecución** → DAG de Airflow aplica cambios en AD
6. **Notificación** → Email de confirmación al solicitante

### Validación por Email

Los validadores reciben emails con enlaces seguros para:
- ✅ Aprobar directamente
- ❌ Rechazar directamente
- 🌐 Revisar en la aplicación web

## 🛠️ API REST

### Endpoints Principales

```http
# Validación por email (GET)
GET /api/validate-permission/{request_id}/{token}?action=approve

# Chat con IA (POST)
POST /api/ai-chat
Content-Type: application/json
{"message": "Solicitar permiso de lectura para \\server\datos"}

# Eventos de auditoría (GET) [Admin]
GET /api/audit-events?user_id=1&event_type=login

# Reporte de permisos (GET) [Admin]
GET /api/permissions-report?folder_id=1

# Búsqueda de carpetas (GET)
GET /api/folders?search=datos&active_only=true

# Búsqueda de grupos AD (GET)
GET /api/ad-groups?search=ventas
```

**Ejemplo de uso:**
```
Usuario: "Necesito permiso de escritura para la carpeta \\server\ventas"
IA: "Te ayudo con esa solicitud. Necesito algunos datos adicionales:
     - ¿Qué grupo de AD utilizarás?
     - ¿Cuál es la justificación de negocio?"
```

## 👥 Roles y Permisos

### Roles del Sistema

- **Administrador** - Acceso completo al sistema y administración
- **Owner** - Propietario de carpetas, puede validar solicitudes
- **Validador** - Puede validar solicitudes para carpetas específicas
- **Usuario** - Puede solicitar permisos y ver sus solicitudes

### Matriz de Permisos

| Acción | Usuario | Validador | Owner | Admin |
|--------|---------|-----------|-------|-------|
| Solicitar permisos | ✅ | ✅ | ✅ | ✅ |
| Ver mis solicitudes | ✅ | ✅ | ✅ | ✅ |
| Validar solicitudes | ❌ | ✅* | ✅* | ✅ |
| Gestionar usuarios | ❌ | ❌ | ❌ | ✅ |
| Gestionar carpetas | ❌ | ❌ | ❌ | ✅ |
| Ver auditoría | ❌ | ❌ | ❌ | ✅ |
| Sincronizar AD | ❌ | ❌ | ❌ | ✅ |

*Solo para carpetas asignadas

## 🔒 Seguridad

### Características de Seguridad

- **Autenticación LDAP** - Credenciales centralizadas
- **Autorización por roles** - Control granular de accesos
- **Protección CSRF** - Tokens en formularios
- **Headers de seguridad** - CSP, HSTS, XSS Protection
- **Tokens seguros** - Para validación por email
- **Auditoría completa** - Registro de todas las acciones
- **Sanitización** - Validación de entrada de datos

### Configuración de Seguridad

```python
# Headers de seguridad configurados
TALISMAN_CONFIG = {
    'force_https': True,  # En producción
    'strict_transport_security': True,
    'content_security_policy': {
        'default-src': "'self'",
        'script-src': "'self' 'unsafe-inline'",
        'style-src': "'self' 'unsafe-inline'"
    }
}
```

## 📝 Logging y Monitoreo

### Logs del Sistema

```bash
# Ver logs de la aplicación
docker-compose logs -f web

# Ver logs de Celery
docker-compose logs -f celery

# Ver logs de base de datos
docker-compose logs -f db
```

### Eventos de Auditoría

Todos los eventos se registran en la tabla `audit_events`:

- Inicios de sesión exitosos/fallidos
- Solicitudes de permisos
- Aprobaciones/rechazos
- Cambios administrativos
- Interacciones con IA
- Sincronizaciones de AD

## 🚀 Despliegue en Producción

### Consideraciones de Producción

1. **Variables de entorno**:
   ```bash
   FLASK_ENV=production
   SECRET_KEY=clave-muy-segura-generada-aleatoriamente
   ```

2. **Base de datos**:
   - Configurar backups automáticos
   - Usar SSL para conexiones
   - Configurar réplicas si es necesario

3. **Seguridad**:
   ```bash
   # Habilitar HTTPS
   TALISMAN_FORCE_HTTPS=true
   
   # Configurar dominio base
   BASE_URL=https://sar.empresa.com
   ```

4. **Escalabilidad**:
   - Usar múltiples workers de Celery
   - Configurar load balancer
   - Usar Redis Cluster si es necesario

### Docker Compose para Producción

```yaml
# docker-compose.prod.yml
version: '3.8'
services:
  web:
    build: .
    restart: always
    environment:
      - FLASK_ENV=production
    volumes:
      - ./logs:/app/logs
      - ./exports:/app/exports
    depends_on:
      - db
      - redis
```

## 🔧 Mantenimiento

### Tareas de Mantenimiento

```bash
# Limpiar archivos de exportación antiguos
docker-compose exec web python -c "from app.services.airflow_service import cleanup_old_export_files; cleanup_old_export_files()"

# Sincronizar grupos de AD
docker-compose exec web python -c "from app.services.ldap_service import LDAPService; LDAPService().sync_groups()"

# Backup de base de datos
docker-compose exec db pg_dump -U saruser sarapp > backup_$(date +%Y%m%d).sql
```

### Monitoreo de Salud

```bash
# Verificar estado de servicios
docker-compose ps

# Verificar conectividad LDAP
docker-compose exec web python -c "from app.services.ldap_service import LDAPService; print(LDAPService().get_connection())"

# Verificar conectividad SMTP
docker-compose exec web python -c "from app.services.email_service import EmailService; EmailService().send_email('test@empresa.com', 'Test', 'Mensaje de prueba')"
```

## 🐛 Troubleshooting

### Problemas Comunes

**Error de conexión LDAP:**
```bash
# Verificar configuración LDAP
docker-compose exec web python -c "from flask import current_app; print(current_app.config['LDAP_HOST'])"
```

**Error de base de datos:**
```bash
# Reinicializar base de datos
docker-compose down
docker volume rm sar_v3_postgres_data
docker-compose up -d
docker-compose exec web python init_db.py
```

**Error de Celery:**
```bash
# Reiniciar workers
docker-compose restart celery
```

## 📚 Desarrollo

### Configuración para Desarrollo

```bash
# Clonar repositorio
git clone <repo-url>
cd SAR_v3

# Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Instalar dependencias
pip install -r requirements.txt

# Configurar variables de entorno para desarrollo
cp .env.example .env
# Editar .env con configuraciones de desarrollo

# Ejecutar aplicación
python app.py
```

### Estructura para Desarrollo

```
SAR_v3/
├── app/
│   ├── __init__.py          # Factory de aplicación
│   ├── models/              # Modelos SQLAlchemy
│   ├── views/               # Blueprints/Controladores
│   ├── forms/               # Formularios WTF
│   ├── services/            # Lógica de negocio
│   ├── utils/               # Utilidades
│   └── templates/           # Plantillas Jinja2
├── tests/                   # Tests unitarios
├── migrations/              # Migraciones Alembic
└── docs/                    # Documentación
```

### Ejecutar Tests

```bash
# Ejecutar tests unitarios
python -m pytest tests/

# Ejecutar con cobertura
python -m pytest --cov=app tests/
```

## 📄 Licencia

Este proyecto está licenciado bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para detalles.

## 🤝 Contribuir

1. Fork el proyecto
2. Crear branch para feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit cambios (`git commit -am 'Agregar nueva funcionalidad'`)
4. Push al branch (`git push origin feature/nueva-funcionalidad`)
5. Crear Pull Request

## 📞 Soporte

Para soporte técnico o preguntas:

- 📧 Email: soporte@empresa.com
- 🐛 Issues: [GitHub Issues](link-to-issues)
- 📖 Wiki: [Documentación](link-to-wiki)

---

**SAR v3.0** - Sistema de Gestión de Permisos de Carpetas