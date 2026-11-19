(() => {
    const key = 'StreamPilotTheme';
    const root = document.documentElement;
    let theme = 'light';

    function updateCharts() {
        const chart = window.Chart;
        if (!chart) return;
        const colors = getComputedStyle(root);
        chart.defaults.color = colors.getPropertyValue('--bs-secondary-color').trim();
        chart.defaults.borderColor = colors.getPropertyValue('--bs-border-color-translucent').trim();
        for (const instance of Object.values(chart.instances)) instance.update('none');
    }

    function apply(value) {
        const next = value === 'dark' ? 'dark' : 'light';
        const changed = theme !== next;
        theme = next;
        root.setAttribute('data-bs-theme', theme);
        const toggle = document.getElementById('themeSwitch');
        if (toggle) toggle.checked = theme === 'dark';
        if (changed) updateCharts();
    }

    function restore() {
        let saved;
        try {
            saved = localStorage.getItem(key);
        } catch (error) {
            if (error.name !== 'SecurityError') throw error;
            return;
        }
        apply(saved);
    }

    restore();
    window.addEventListener('pageshow', restore);
    window.addEventListener('storage', (event) => {
        if (event.key === key || event.key === null) {
            restore();
        }
    });
    document.addEventListener('DOMContentLoaded', () => {
        updateCharts();
        const toggle = document.getElementById('themeSwitch');
        if (!toggle) return;
        toggle.checked = theme === 'dark';
        toggle.addEventListener('change', () => {
            apply(toggle.checked ? 'dark' : 'light');
            try {
                localStorage.setItem(key, theme);
            } catch (error) {
                if (error.name !== 'SecurityError' && error.name !== 'QuotaExceededError') throw error;
                // The current page still works when browser storage is disabled.
            }
        });
    });
})();
