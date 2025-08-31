from flask_wtf import FlaskForm
from wtforms import StringField, TextAreaField, BooleanField, SelectField, SubmitField, SelectMultipleField
from wtforms.validators import DataRequired, Length, Email, Optional
from app.models import Role, User, ADGroup

class UserForm(FlaskForm):
    username = StringField('Usuario', validators=[
        DataRequired(message='El usuario es requerido'),
        Length(min=3, max=80, message='El usuario debe tener entre 3 y 80 caracteres')
    ])
    email = StringField('Email', validators=[
        DataRequired(message='El email es requerido'),
        Email(message='Formato de email inválido'),
        Length(max=120, message='El email no puede exceder 120 caracteres')
    ])
    full_name = StringField('Nombre Completo', validators=[
        DataRequired(message='El nombre completo es requerido'),
        Length(max=200, message='El nombre no puede exceder 200 caracteres')
    ])
    department = StringField('Departamento', validators=[
        Length(max=100, message='El departamento no puede exceder 100 caracteres')
    ])
    is_active = BooleanField('Activo', default=True)
    roles = SelectMultipleField('Roles', coerce=int)
    submit = SubmitField('Guardar Usuario')
    
    def __init__(self, *args, **kwargs):
        super(UserForm, self).__init__(*args, **kwargs)
        self.roles.choices = [(r.id, r.name) for r in Role.query.all()]

class FolderForm(FlaskForm):
    name = StringField('Nombre', validators=[
        DataRequired(message='El nombre es requerido'),
        Length(max=255, message='El nombre no puede exceder 255 caracteres')
    ])
    path = StringField('Ruta', validators=[
        DataRequired(message='La ruta es requerida'),
        Length(max=500, message='La ruta no puede exceder 500 caracteres')
    ])
    description = TextAreaField('Descripción', validators=[
        Length(max=1000, message='La descripción no puede exceder 1000 caracteres')
    ])
    is_active = BooleanField('Activa', default=True)
    owners = SelectMultipleField('Propietarios', coerce=int)
    validators = SelectMultipleField('Validadores', coerce=int)
    read_groups = SelectMultipleField('Grupos de Lectura', coerce=int)
    write_groups = SelectMultipleField('Grupos de Escritura', coerce=int)
    submit = SubmitField('Guardar Carpeta')
    
    def __init__(self, *args, **kwargs):
        super(FolderForm, self).__init__(*args, **kwargs)
        users = User.query.filter_by(is_active=True).all()
        ad_groups = ADGroup.query.filter_by(is_active=True).all()
        
        self.owners.choices = [(u.id, f"{u.full_name} ({u.username})") for u in users]
        self.validators.choices = [(u.id, f"{u.full_name} ({u.username})") for u in users]
        self.read_groups.choices = [(g.id, f"{g.name}") for g in ad_groups]
        self.write_groups.choices = [(g.id, f"{g.name}") for g in ad_groups]

class ADGroupForm(FlaskForm):
    name = StringField('Nombre', validators=[
        DataRequired(message='El nombre es requerido'),
        Length(max=200, message='El nombre no puede exceder 200 caracteres')
    ])
    distinguished_name = StringField('Distinguished Name', validators=[
        DataRequired(message='El DN es requerido'),
        Length(max=500, message='El DN no puede exceder 500 caracteres')
    ])
    description = TextAreaField('Descripción', validators=[
        Length(max=1000, message='La descripción no puede exceder 1000 caracteres')
    ])
    group_type = SelectField('Tipo de Grupo', choices=[
        ('Security', 'Seguridad'),
        ('Distribution', 'Distribución')
    ], default='Security')
    is_active = BooleanField('Activo', default=True)
    submit = SubmitField('Guardar Grupo')