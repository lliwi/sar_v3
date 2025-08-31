# Sistema de Temas SAR

Este directorio contiene el sistema de temas personalizable para la aplicación SAR.

## Estructura de Archivos

- `variables.css` - Variables CSS globales y configuración base
- `main.css` - Estilos principales de la aplicación
- `themes.css` - Definiciones de temas disponibles
- `theme-manager.js` - JavaScript para gestión de temas

## Variables de Color Principales

Las siguientes variables CSS pueden ser modificadas para cambiar el tema:

### Colores Primarios
```css
--primary-color: #007bff;      /* Color principal */
--primary-dark: #0056b3;       /* Color principal oscuro */
--primary-light: #66b3ff;      /* Color principal claro */
```

### Colores Secundarios
```css
--secondary-color: #6c757d;    /* Color secundario */
--secondary-dark: #495057;     /* Color secundario oscuro */
--secondary-light: #adb5bd;    /* Color secundario claro */
```

### Colores de Estado
```css
--success-color: #28a745;      /* Verde para éxito */
--warning-color: #ffc107;      /* Amarillo para advertencias */
--danger-color: #dc3545;       /* Rojo para errores */
--info-color: #17a2b8;         /* Azul para información */
```

## Temas Disponibles

### 1. Default (Azul)
- Color principal: `#007bff`
- Ideal para: Aplicaciones corporativas estándar

### 2. Corporate (Azul Corporativo)
- Color principal: `#1f4e79`
- Ideal para: Entornos empresariales formales

### 3. Green (Verde)
- Color principal: `#28a745`
- Ideal para: Aplicaciones relacionadas con naturaleza/sostenibilidad

### 4. Purple (Púrpura)
- Color principal: `#6f42c1`
- Ideal para: Aplicaciones creativas

### 5. Orange (Naranja)
- Color principal: `#fd7e14`
- Ideal para: Aplicaciones energéticas

### 6. Red (Rojo)
- Color principal: `#dc3545`
- Ideal para: Aplicaciones que requieren atención

### 7. Teal (Verde Azulado)
- Color principal: `#20c997`
- Ideal para: Aplicaciones modernas

### 8. Dark (Oscuro)
- Color principal: `#0d6efd`
- Ideal para: Uso nocturno o preferencia de tema oscuro

### 9. High Contrast (Alto Contraste)
- Color principal: `#0000ff`
- Ideal para: Accesibilidad mejorada

## Cómo Crear un Nuevo Tema

1. **Agregar definición en `themes.css`:**
```css
[data-theme="mi-tema"] {
    --primary-color: #tu-color-principal;
    --primary-dark: #tu-color-principal-oscuro;
    --primary-light: #tu-color-principal-claro;
    /* ... más variables según necesites */
}
```

2. **Registrar el tema en `theme-manager.js`:**
```javascript
this.themes = [
    // ... temas existentes
    { id: 'mi-tema', name: 'Mi Tema Personalizado', color: '#tu-color-principal' }
];
```

## Uso Programático

### JavaScript
```javascript
// Cambiar tema
window.themeManager.applyTheme('corporate');

// Obtener tema actual
const currentTheme = window.themeManager.getCurrentTheme();

// Alternar modo oscuro
window.themeManager.toggleDarkMode();

// Escuchar cambios de tema
document.addEventListener('themeChanged', (e) => {
    console.log('Nuevo tema:', e.detail.theme);
});
```

### HTML/CSS
```html
<!-- Aplicar tema específico a un elemento -->
<div data-theme="green">
    <!-- Este contenido usará el tema verde -->
</div>
```

## Personalización Avanzada

### Variables Adicionales Disponibles

#### Espaciado
```css
--spacing-xs: 0.25rem;
--spacing-sm: 0.5rem;
--spacing-md: 1rem;
--spacing-lg: 1.5rem;
--spacing-xl: 3rem;
```

#### Tipografía
```css
--font-size-xs: 0.75rem;
--font-size-sm: 0.875rem;
--font-size-base: 1rem;
--font-size-lg: 1.25rem;
--font-size-xl: 1.5rem;
--font-size-xxl: 2rem;
```

#### Bordes y Sombras
```css
--border-radius-sm: 0.2rem;
--border-radius: 0.375rem;
--border-radius-lg: 0.5rem;
--border-radius-xl: 1rem;

--shadow-sm: 0 0.125rem 0.25rem rgba(0, 0, 0, 0.075);
--shadow: 0 0.5rem 1rem rgba(0, 0, 0, 0.15);
--shadow-lg: 0 1rem 3rem rgba(0, 0, 0, 0.175);
```

#### Transiciones
```css
--transition-fast: 0.15s ease-in-out;
--transition-normal: 0.3s ease-in-out;
--transition-slow: 0.5s ease-in-out;
```

## Soporte de Modo Oscuro Automático

El sistema incluye soporte para detección automática del modo oscuro del sistema:

```css
@media (prefers-color-scheme: dark) {
    /* Estilos automáticos para modo oscuro */
}
```

## Persistencia

Los temas seleccionados se guardan automáticamente en `localStorage` y se restauran al recargar la página.

## Accesibilidad

- El tema "High Contrast" está optimizado para accesibilidad
- Todos los colores cumplen con las pautas WCAG 2.1
- Soporte para lectores de pantalla incluido

## Ejemplo de Uso Completo

```html
<!DOCTYPE html>
<html data-theme="corporate">
<head>
    <link rel="stylesheet" href="css/variables.css">
    <link rel="stylesheet" href="css/themes.css">
    <link rel="stylesheet" href="css/main.css">
</head>
<body>
    <!-- Tu contenido aquí -->
    <script src="js/theme-manager.js"></script>
</body>
</html>
```

## Mantenimiento

Para mantener consistencia:
1. Siempre usa variables CSS en lugar de valores hardcodeados
2. Testea nuevos temas en todos los componentes
3. Verifica accesibilidad con herramientas apropiadas
4. Documenta cualquier nueva variable agregada