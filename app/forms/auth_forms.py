from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, BooleanField
from wtforms.validators import DataRequired, Length

class LoginForm(FlaskForm):
    username = StringField('Usuario', validators=[
        DataRequired(message='El usuario es requerido'),
        Length(min=3, max=80, message='El usuario debe tener entre 3 y 80 caracteres')
    ], render_kw={'autocomplete': 'username'})
    password = PasswordField('Contraseña', validators=[
        DataRequired(message='La contraseña es requerida')
    ], render_kw={'autocomplete': 'current-password'})
    remember_me = BooleanField('Recordarme')
    submit = SubmitField('Iniciar Sesión')