/**
 * SAR Application - Theme Manager
 * Handles theme switching and persistence
 */

class ThemeManager {
    constructor() {
        // Fixed to corporate theme only
        this.currentTheme = 'corporate';
        this.init();
    }
    
    init() {
        // Check if theme is already applied in HTML to prevent FOUC
        const currentDataTheme = document.documentElement.getAttribute('data-theme');
        if (!currentDataTheme || currentDataTheme !== this.currentTheme) {
            this.applyTheme(this.currentTheme);
        }
    }
    
    /**
     * Apply a theme to the document
     */
    applyTheme(themeId) {
        document.documentElement.setAttribute('data-theme', themeId);
        this.currentTheme = themeId;
        
        // Dispatch custom event for theme change
        document.dispatchEvent(new CustomEvent('themeChanged', {
            detail: { theme: themeId }
        }));
    }
    
    /**
     * Get current theme
     */
    getCurrentTheme() {
        return this.currentTheme;
    }
}

// Initialize theme manager when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.themeManager = new ThemeManager();
});

// Listen for theme changes
document.addEventListener('themeChanged', (e) => {
    console.log('Theme changed to:', e.detail.theme);
});

// Export for global access
window.ThemeManager = ThemeManager;