# 🎨 Sistema de Temas SAR - Guía de Uso

## Resumen

Se ha implementado un sistema completo de temas para la aplicación SAR que permite cambiar fácilmente los colores y la apariencia de la aplicación.

## ✨ Características Implementadas

### 1. **CSS Modular Separado**
- ✅ **`variables.css`** - Variables CSS globales
- ✅ **`main.css`** - Estilos principales de la aplicación  
- ✅ **`themes.css`** - Definiciones de múltiples temas
- ✅ **`theme-manager.js`** - JavaScript para gestión de temas

### 2. **Variables de Color Centralizadas**
```css
/* Colores principales */
--primary-color: #007bff;      /* Azul principal */
--secondary-color: #6c757d;    /* Gris secundario */

/* Colores de estado */
--success-color: #28a745;      /* Verde */
--warning-color: #ffc107;      /* Amarillo */
--danger-color: #dc3545;       /* Rojo */
--info-color: #17a2b8;         /* Azul info */
```

### 3. **9 Temas Predefinidos**

| Tema | Color Principal | Descripción |
|------|-----------------|-------------|
| **Default** | `#007bff` | Azul estándar (Bootstrap) |
| **Corporate** | `#1f4e79` | Azul corporativo formal |
| **Green** | `#28a745` | Verde natural/eco |
| **Purple** | `#6f42c1` | Púrpura creativo |
| **Orange** | `#fd7e14` | Naranja energético |
| **Red** | `#dc3545` | Rojo llamativo |
| **Teal** | `#20c997` | Verde azulado moderno |
| **Dark** | `#0d6efd` | Modo oscuro |
| **High Contrast** | `#0000ff` | Alto contraste (accesibilidad) |

## 🚀 Cómo Usar

### **Método 1: Selector Automático en la Interfaz**
Una vez que inicies sesión, aparecerá automáticamente un selector de temas en la barra de navegación con un icono de paleta 🎨.

### **Método 2: JavaScript Manual**
```javascript
// Cambiar a tema corporativo
window.themeManager.applyTheme('corporate');

// Cambiar a tema verde
window.themeManager.applyTheme('green');

// Alternar modo oscuro
window.themeManager.toggleDarkMode();
```

### **Método 3: HTML Directo**
```html
<body data-theme="purple">
    <!-- El contenido usará el tema púrpura -->
</body>
```

## 🛠️ Cómo Crear Tu Propio Tema

### **Paso 1: Definir el Tema en CSS**
Agrega a `app/static/css/themes.css`:

```css
[data-theme="mi-tema-personalizado"] {
    --primary-color: #ff6b6b;        /* Tu color principal */
    --primary-dark: #ee5a52;         /* Versión más oscura */
    --primary-light: #ff8e8e;        /* Versión más clara */
    
    /* Opcional: personalizar sidebar */
    --sidebar-bg: #2c1810;
    --sidebar-active-bg: #3c2820;
}
```

### **Paso 2: Registrar en JavaScript**
Edita `app/static/js/theme-manager.js` y agrega a la lista `this.themes`:

```javascript
{ 
    id: 'mi-tema-personalizado', 
    name: 'Mi Tema Único', 
    color: '#ff6b6b' 
}
```

## 🎯 Casos de Uso Recomendados

### **Para Diferentes Departamentos:**
- **IT/Tecnología** → `corporate` (azul formal)
- **Finanzas** → `green` (verde confiable)  
- **Marketing** → `purple` (púrpura creativo)
- **Ventas** → `orange` (naranja energético)
- **Operaciones** → `teal` (moderno)

### **Para Diferentes Horarios:**
- **Día** → `default` o `corporate`
- **Noche** → `dark` (automático con `prefers-color-scheme`)

### **Para Accesibilidad:**
- **Usuarios con dificultades visuales** → `high-contrast`

## 💾 Persistencia

- **Automática**: Los temas se guardan en `localStorage`
- **Por usuario**: Cada navegador recuerda la preferencia
- **Restauración**: Se aplica automáticamente al recargar

## 📱 Diseño Responsivo

Todos los temas incluyen:
- ✅ Diseño responsivo para móviles
- ✅ Sidebar colapsable en pantallas pequeñas
- ✅ Colores optimizados para diferentes tamaños de pantalla

## 🔧 Variables Adicionales Personalizables

```css
/* Espaciado */
--spacing-xs: 0.25rem;
--spacing-sm: 0.5rem;
--spacing-md: 1rem;
--spacing-lg: 1.5rem;
--spacing-xl: 3rem;

/* Tipografía */
--font-size-xs: 0.75rem;
--font-size-base: 1rem;
--font-size-lg: 1.25rem;
--font-size-xl: 1.5rem;

/* Bordes y efectos */
--border-radius: 0.375rem;
--shadow: 0 0.5rem 1rem rgba(0, 0, 0, 0.15);
--transition-fast: 0.15s ease-in-out;
```

## 🧪 Cómo Probar

1. **Accede a la aplicación**: http://localhost:8080
2. **Inicia sesión** con cualquier usuario (ej: `admin.user` / `admin123`)
3. **Busca el selector de temas** (icono de paleta) en la barra de navegación
4. **Prueba diferentes temas** y observa los cambios inmediatos
5. **Recarga la página** para verificar que el tema se mantiene

## 📁 Estructura de Archivos Creados

```
app/static/
├── css/
│   ├── variables.css      # Variables CSS globales
│   ├── main.css          # Estilos principales  
│   ├── themes.css        # Definiciones de temas
│   └── README.md         # Documentación técnica
└── js/
    └── theme-manager.js   # Gestor JavaScript de temas
```

## 🔄 Migración desde el CSS Inline

- ✅ **Removido**: CSS inline del template `base.html`
- ✅ **Separado**: Estilos en archivos independientes
- ✅ **Mejorado**: Sistema de variables más flexible
- ✅ **Optimizado**: Carga más eficiente y mantenimiento simplificado

## 🎉 Beneficios Obtenidos

1. **Facilidad de mantenimiento** - CSS organizado y modular
2. **Personalización rápida** - Cambiar colores desde variables
3. **Múltiples temas** - 9 opciones predefinidas
4. **Persistencia** - Los usuarios mantienen su preferencia
5. **Accesibilidad** - Tema de alto contraste incluido
6. **Escalabilidad** - Fácil agregar nuevos temas
7. **Performance** - CSS separado permite mejor caching

¡El sistema está listo para usar! 🚀