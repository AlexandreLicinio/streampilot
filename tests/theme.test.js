const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const script = fs.readFileSync(path.join(__dirname, '../streampilot/static/theme.js'), 'utf8');

function page(storage, chart = null) {
    const listeners = {};
    const attributes = {};
    const toggle = {checked: false, addEventListener: (name, fn) => { listeners[name] = fn; }};
    const root = {setAttribute: (name, value) => { attributes[name] = value; }};
    const context = {
        document: {
            documentElement: root,
            getElementById: () => toggle,
            addEventListener: (name, fn) => { listeners[name] = fn; },
        },
        window: {Chart: chart, addEventListener: (name, fn) => { listeners[name] = fn; }},
        localStorage: storage,
        getComputedStyle: () => ({getPropertyValue: () => ' color '}),
    };
    vm.runInNewContext(script, context);
    listeners.DOMContentLoaded();
    return {attributes, toggle, listeners};
}

test('theme persists across pages and synchronizes on storage and pageshow', () => {
    let saved = 'dark';
    const storage = {
        getItem: () => saved,
        setItem: (key, value) => { assert.equal(key, 'StreamPilotTheme'); saved = value; },
    };
    let updates = 0;
    const chart = {defaults: {}, instances: {one: {update: () => { updates++; }}}};
    const first = page(storage, chart);
    assert.equal(first.attributes['data-bs-theme'], 'dark');
    assert.equal(first.toggle.checked, true);
    first.toggle.checked = false;
    first.listeners.change();
    assert.equal(saved, 'light');
    assert.equal(first.attributes['data-bs-theme'], 'light');
    assert.equal(chart.defaults.color, 'color');
    assert.ok(updates >= 2);

    const second = page(storage);
    assert.equal(second.attributes['data-bs-theme'], 'light');
    saved = 'dark';
    second.listeners.storage({key: 'StreamPilotTheme'});
    assert.equal(second.toggle.checked, true);
    saved = 'light';
    second.listeners.pageshow();
    assert.equal(second.toggle.checked, false);
});

test('switch still works when storage is disabled', () => {
    const error = Object.assign(new Error('disabled'), {name: 'SecurityError'});
    const view = page({getItem: () => { throw error; }, setItem: () => { throw error; }});
    view.toggle.checked = true;
    view.listeners.change();
    assert.equal(view.attributes['data-bs-theme'], 'dark');
});
