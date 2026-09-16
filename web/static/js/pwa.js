let deferredInstallPrompt = null;

function isStandalonePwa() {
    return window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone === true;
}

function setInstallButtonState(isReady) {
    const installButton = document.getElementById('install-btn');
    if (!installButton) return;

    const shouldHide = isStandalonePwa() || !isReady;
    installButton.classList.toggle('hidden', shouldHide);
    installButton.classList.toggle('is-ready', !shouldHide);
    installButton.setAttribute('aria-hidden', shouldHide ? 'true' : 'false');
}

async function handleInstallButtonClick() {
    if (!deferredInstallPrompt) return;
    deferredInstallPrompt.prompt();
    const { outcome } = await deferredInstallPrompt.userChoice;
    console.log(`[PWA] Install prompt outcome: ${outcome}`);
    deferredInstallPrompt = null;
    setInstallButtonState(false);
}

// --- Service Worker Registration ---
// Register on every page load so push works in both web and PWA contexts.
if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/scanner/sw.js', { scope: '/scanner/' })
        .then(reg => {
            console.log('[SW] Registered, scope:', reg.scope);
            // Force the new service worker to activate immediately
            reg.update();
            if (reg.waiting) {
                reg.waiting.postMessage({ type: 'SKIP_WAITING' });
            }
            reg.addEventListener('updatefound', () => {
                const newWorker = reg.installing;
                if (newWorker) {
                    newWorker.addEventListener('statechange', () => {
                        if (newWorker.state === 'installed' && navigator.serviceWorker.controller) {
                            newWorker.postMessage({ type: 'SKIP_WAITING' });
                        }
                    });
                }
            });
        })
        .catch(err => console.warn('[SW] Registration failed:', err));
}

/**
 * Returns navigator.serviceWorker.ready with a timeout.
 * Avoids hanging forever when the SW hasn't activated yet.
 */
function swReady(timeoutMs = 10000) {
    return Promise.race([
        navigator.serviceWorker.ready,
        new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Service worker not ready within timeout')), timeoutMs)
        ),
    ]);
}

// --- PWA Installation Logic ---
window.addEventListener('beforeinstallprompt', (event) => {
    event.preventDefault();
    deferredInstallPrompt = event;
    setInstallButtonState(true);
});
window.addEventListener('appinstalled', () => {
    console.log('[PWA] App installed');
    deferredInstallPrompt = null;
    document.body.classList.add('pwa-standalone');
    setInstallButtonState(false);
});

// --- iOS "Add to Home Screen" Banner ---
(function () {
    const isIos = /iphone|ipad|ipod/i.test(navigator.userAgent);
    const isStandalone = window.navigator.standalone === true;
    const dismissed = localStorage.getItem('ios-install-dismissed');
    if (!isIos || isStandalone || dismissed) return;

    const banner = document.createElement('div');
    banner.id = 'ios-install-banner';
    banner.innerHTML = `
        <span>Install this app: tap the <strong>Share</strong> button &#x2197; then <strong>"Add to Home Screen"</strong></span>
        <button id="ios-install-dismiss" aria-label="Dismiss">&times;</button>
    `;
    const btn = banner.querySelector('#ios-install-dismiss');

    document.addEventListener('DOMContentLoaded', () => {
        document.body.appendChild(banner);
        btn.addEventListener('click', () => {
            banner.remove();
            localStorage.setItem('ios-install-dismissed', '1');
        });
    });
})();

// --- Scanner alert setup (shared by the browser and installed PWA) ---

const NOTIFICATION_FEEDS_STORAGE = 'scanner_notification_feeds_v2';
const NOTIFICATION_MESSAGE_MODE_STORAGE = 'scanner_notification_message_mode_v1';
const DEFAULT_NOTIFICATION_FEEDS = ['pd', 'fd', 'mpd', 'mfd'];
const DEFAULT_NOTIFICATION_MESSAGE_MODE = 'transcript';
let currentNotificationState = {
    supported: false,
    permission: 'default',
    subscribed: false,
    subscription: null,
};

function _urlBase64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - base64String.length % 4) % 4);
    const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
    const rawData = atob(base64);
    return Uint8Array.from([...rawData].map(character => character.charCodeAt(0)));
}

function notificationSupported() {
    return 'Notification' in window && 'serviceWorker' in navigator && 'PushManager' in window;
}

function savedNotificationFeeds() {
    try {
        const saved = JSON.parse(localStorage.getItem(NOTIFICATION_FEEDS_STORAGE) || 'null');
        return Array.isArray(saved) ? saved : null;
    } catch (_) {
        return null;
    }
}

function rememberNotificationFeeds(feeds) {
    localStorage.setItem(NOTIFICATION_FEEDS_STORAGE, JSON.stringify(feeds));
}

function savedNotificationMessageMode() {
    const mode = localStorage.getItem(NOTIFICATION_MESSAGE_MODE_STORAGE);
    return ['alert_only', 'transcript'].includes(mode) ? mode : null;
}

function rememberNotificationMessageMode(mode) {
    localStorage.setItem(NOTIFICATION_MESSAGE_MODE_STORAGE, mode);
}

async function fetchJson(url, options) {
    const response = await fetch(url, options);
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
    return data;
}

async function getNotificationState() {
    const supported = notificationSupported();
    if (!supported) {
        return { supported: false, permission: 'unsupported', subscribed: false, subscription: null };
    }
    let subscription = null;
    try {
        const registration = await swReady();
        subscription = await registration.pushManager.getSubscription();
    } catch (error) {
        console.warn('[Alerts] Could not inspect subscription:', error);
    }
    return {
        supported: true,
        permission: Notification.permission,
        subscribed: Boolean(subscription && Notification.permission === 'granted'),
        subscription,
    };
}

async function getVapidPublicKey() {
    const response = await fetch('/scanner/push/vapid_public', { cache: 'no-store' });
    if (!response.ok) throw new Error('Alert service is not configured.');
    return (await response.text()).trim();
}

async function enableNotifications(feeds, messageMode) {
    if (!notificationSupported()) throw new Error('This browser does not support scanner alerts.');
    if (Notification.permission === 'denied') {
        throw new Error('Alerts are blocked in browser settings.');
    }
    if (Notification.permission !== 'granted') {
        const permission = await Notification.requestPermission();
        if (permission !== 'granted') throw new Error('Alerts were not enabled.');
    }

    const registration = await swReady();
    let subscription = await registration.pushManager.getSubscription();
    if (!subscription) {
        const applicationServerKey = _urlBase64ToUint8Array(await getVapidPublicKey());
        subscription = await registration.pushManager.subscribe({
            userVisibleOnly: true,
            applicationServerKey,
        });
    }

    await fetchJson('/scanner/push/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            subscription: subscription.toJSON(),
            feeds,
            message_mode: messageMode,
        }),
    });
    rememberNotificationFeeds(feeds);
    rememberNotificationMessageMode(messageMode);
    return subscription;
}

async function saveNotificationPreferences(subscription, feeds, messageMode) {
    await fetchJson('/scanner/push/prefs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            endpoint: subscription.endpoint,
            feeds,
            message_mode: messageMode,
        }),
    });
    rememberNotificationFeeds(feeds);
    rememberNotificationMessageMode(messageMode);
}

async function disableNotifications(subscription) {
    if (!subscription) return;
    await fetchJson('/scanner/push/unsubscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ endpoint: subscription.endpoint }),
    });
    await subscription.unsubscribe();
}

function updateNotificationControls(state) {
    const label = !state.supported
        ? 'Unavailable'
        : state.permission === 'denied'
            ? 'Blocked'
            : state.subscribed ? 'On' : 'Off';
    document.querySelectorAll('[data-notification-status]').forEach(element => {
        element.textContent = label;
    });
    document.querySelectorAll('[data-notification-control]').forEach(element => {
        element.classList.toggle('notification-control-active', state.subscribed);
        element.classList.toggle('notification-control-blocked', state.permission === 'denied');
        element.setAttribute('aria-label', `Open scanner alert settings. Alerts are ${label.toLowerCase()}.`);
    });
}

async function refreshNotificationControls() {
    currentNotificationState = await getNotificationState();
    updateNotificationControls(currentNotificationState);
    return currentNotificationState;
}

function escapeNotificationHTML(value) {
    return String(value ?? '').replace(/[&<>"']/g, character => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    })[character]);
}

function initNotificationDialog() {
    const overlay = document.getElementById('notif-overlay');
    if (!overlay || overlay.dataset.notifInitialised === '1') return;
    overlay.dataset.notifInitialised = '1';

    const dialog = overlay.querySelector('[role="dialog"]');
    const backdrop = document.getElementById('notif-backdrop');
    const closeButton = document.getElementById('notif-close');
    const cancelButton = document.getElementById('notif-cancel');
    const primaryButton = document.getElementById('notif-primary');
    const disableButton = document.getElementById('notif-disable');
    const channelList = document.getElementById('notif-channel-list');
    const selectionSummary = document.getElementById('notif-selection-summary');
    const statusLabel = document.getElementById('notif-status-label');
    const statusTitle = document.getElementById('notif-status-title');
    const statusCopy = document.getElementById('notif-status-copy');
    const statusCard = document.getElementById('notif-status-card');
    const message = document.getElementById('notif-message');
    let channels = [];
    let previousFocus = null;

    function selectedFeeds() {
        return [...channelList.querySelectorAll('.notif-feed-input:checked')]
            .map(input => input.dataset.feed);
    }

    function selectedMessageMode() {
        return overlay.querySelector('input[name="notif-message-mode"]:checked')?.value
            || DEFAULT_NOTIFICATION_MESSAGE_MODE;
    }

    function setMessage(text, kind = '') {
        message.textContent = text;
        message.className = `notif-message${kind ? ` is-${kind}` : ''}`;
    }

    function syncTownToggles() {
        channelList.querySelectorAll('.notif-town-input').forEach(townInput => {
            const inputs = [...channelList.querySelectorAll(`.notif-feed-input[data-town="${townInput.dataset.town}"]`)];
            const checked = inputs.filter(input => input.checked).length;
            townInput.checked = checked === inputs.length && inputs.length > 0;
            townInput.indeterminate = checked > 0 && checked < inputs.length;
        });
    }

    function updateSelectionSummary() {
        syncTownToggles();
        const feeds = selectedFeeds();
        const towns = new Set(channels.filter(channel => feeds.includes(channel.id)).map(channel => channel.town));
        selectionSummary.textContent = feeds.length
            ? `${feeds.length} feed${feeds.length === 1 ? '' : 's'} across ${towns.size} town${towns.size === 1 ? '' : 's'}`
            : 'Choose at least one feed';
        primaryButton.disabled = !currentNotificationState.supported || currentNotificationState.permission === 'denied' || feeds.length === 0;
        rememberNotificationFeeds(feeds);
    }

    function renderChannels(selected) {
        const towns = new Map();
        channels.forEach(channel => {
            if (!towns.has(channel.town)) towns.set(channel.town, []);
            towns.get(channel.town).push(channel);
        });
        channelList.innerHTML = [...towns.entries()].map(([town, townChannels]) => {
            const townKey = town.toLowerCase().replace(/[^a-z0-9]+/g, '-');
            const choices = townChannels.map(channel => {
                const department = channel.type === 'fire' ? 'Fire' : 'Police';
                return `<label class="notif-feed-choice">
                    <input class="notif-feed-input" type="checkbox" data-feed="${escapeNotificationHTML(channel.id)}" data-town="${escapeNotificationHTML(townKey)}" ${selected.includes(channel.id) ? 'checked' : ''}>
                    <span class="notif-type-dot ${channel.type === 'fire' ? 'dot-fire' : 'dot-police'}" aria-hidden="true"></span>
                    <span>${department}</span>
                </label>`;
            }).join('');
            return `<fieldset class="notif-town-group">
                <legend class="sr-only">${escapeNotificationHTML(town)} alerts</legend>
                <label class="notif-town-choice">
                    <input class="notif-town-input" type="checkbox" data-town="${escapeNotificationHTML(townKey)}">
                    <span>${escapeNotificationHTML(town)}</span>
                </label>
                <div class="notif-feed-choices">${choices}</div>
            </fieldset>`;
        }).join('');
        updateSelectionSummary();
    }

    function renderStatus(state) {
        statusCard.classList.toggle('is-on', state.subscribed);
        statusCard.classList.toggle('is-blocked', state.permission === 'denied');
        disableButton.classList.toggle('hidden', !state.subscribed);
        if (!state.supported) {
            statusLabel.textContent = 'Unavailable';
            statusTitle.textContent = 'Alerts are not supported here';
            statusCopy.textContent = 'Try the current version of Chrome, Edge, Firefox, or Safari.';
            primaryButton.textContent = 'Alerts unavailable';
        } else if (state.permission === 'denied') {
            statusLabel.textContent = 'Blocked';
            statusTitle.textContent = 'Allow alerts in browser settings';
            statusCopy.textContent = 'Open this site’s permissions, allow notifications, then return here.';
            primaryButton.textContent = 'Blocked in settings';
        } else if (state.subscribed) {
            statusLabel.textContent = 'On for this device';
            statusTitle.textContent = 'Local scanner alerts are active';
            statusCopy.textContent = 'We group activity by feed and limit alert frequency.';
            primaryButton.textContent = 'Save preferences';
        } else {
            statusLabel.textContent = 'Off';
            statusTitle.textContent = 'Get only the local alerts you choose';
            statusCopy.textContent = 'Your browser asks for permission only after you select Turn on alerts.';
            primaryButton.textContent = 'Turn on alerts';
        }
        updateSelectionSummary();
    }

    async function loadDialog() {
        channelList.innerHTML = '<p class="notif-loading">Loading local feeds…</p>';
        setMessage('');
        primaryButton.disabled = true;
        try {
            const [channelData, state] = await Promise.all([
                fetchJson('/scanner/push/channels', { cache: 'no-store' }),
                refreshNotificationControls(),
            ]);
            channels = channelData.channels || [];
            let feeds = savedNotificationFeeds();
            let messageMode = savedNotificationMessageMode() || DEFAULT_NOTIFICATION_MESSAGE_MODE;
            if (state.subscribed && state.subscription) {
                try {
                    const preferences = await fetchJson(`/scanner/push/prefs?endpoint=${encodeURIComponent(state.subscription.endpoint)}`, { cache: 'no-store' });
                    feeds = preferences.feeds;
                    messageMode = preferences.message_mode || messageMode;
                } catch (error) {
                    console.warn('[Alerts] Could not load saved preferences:', error);
                }
            }
            const messageModeInput = overlay.querySelector(`input[name="notif-message-mode"][value="${messageMode}"]`);
            if (messageModeInput) messageModeInput.checked = true;
            rememberNotificationMessageMode(messageMode);
            const validIds = new Set(channels.map(channel => channel.id));
            const selected = (feeds || DEFAULT_NOTIFICATION_FEEDS).filter(feed => validIds.has(feed));
            renderChannels(selected);
            renderStatus(state);
        } catch (error) {
            console.error('[Alerts] Setup failed:', error);
            channelList.innerHTML = '<p class="notif-loading is-error">Local feeds could not be loaded.</p>';
            setMessage('Close this panel and try again.', 'error');
        }
    }

    function openDialog(event) {
        event?.preventDefault?.();
        event?.stopPropagation?.();
        const activeElement = document.activeElement;
        previousFocus = activeElement && activeElement.closest?.('#menu-dropdown')
            ? document.getElementById('mobile-more-btn')
            : activeElement;
        window.setScannerMoreMenuOpen?.(false);
        overlay.classList.remove('hidden');
        requestAnimationFrame(() => closeButton.focus());
        loadDialog();
    }

    function closeDialog() {
        overlay.classList.add('hidden');
        if (previousFocus instanceof HTMLElement) previousFocus.focus();
        previousFocus = null;
    }

    function trapDialogFocus(event) {
        if (event.key === 'Escape') {
            event.preventDefault();
            closeDialog();
            return;
        }
        if (event.key !== 'Tab') return;
        const focusable = [...dialog.querySelectorAll('button:not([disabled]), input:not([disabled]), [tabindex]:not([tabindex="-1"])')]
            .filter(element => !element.closest('.hidden'));
        if (!focusable.length) return;
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (event.shiftKey && document.activeElement === first) {
            event.preventDefault();
            last.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
            event.preventDefault();
            first.focus();
        }
    }

    document.querySelectorAll('[data-notification-control]').forEach(button => button.addEventListener('click', openDialog));
    closeButton.addEventListener('click', closeDialog);
    cancelButton.addEventListener('click', closeDialog);
    backdrop.addEventListener('click', closeDialog);
    dialog.addEventListener('keydown', trapDialogFocus);

    channelList.addEventListener('change', event => {
        const townInput = event.target.closest('.notif-town-input');
        if (townInput) {
            channelList.querySelectorAll(`.notif-feed-input[data-town="${townInput.dataset.town}"]`)
                .forEach(input => { input.checked = townInput.checked; });
        }
        setMessage('');
        updateSelectionSummary();
    });

    overlay.querySelectorAll('input[name="notif-message-mode"]').forEach(input => {
        input.addEventListener('change', () => {
            rememberNotificationMessageMode(selectedMessageMode());
            setMessage('');
        });
    });

    document.getElementById('notif-quick-select').addEventListener('click', event => {
        const button = event.target.closest('[data-notif-select]');
        if (!button) return;
        const mode = button.dataset.notifSelect;
        channelList.querySelectorAll('.notif-feed-input').forEach(input => {
            const channel = channels.find(item => item.id === input.dataset.feed);
            input.checked = mode === 'all' || channel?.type === mode;
        });
        setMessage('');
        updateSelectionSummary();
    });

    primaryButton.addEventListener('click', async () => {
        const feeds = selectedFeeds();
        const messageMode = selectedMessageMode();
        if (!feeds.length) {
            setMessage('Choose at least one Police or Fire feed.', 'error');
            channelList.querySelector('input')?.focus();
            return;
        }
        primaryButton.disabled = true;
        disableButton.disabled = true;
        setMessage(currentNotificationState.subscribed ? 'Saving your choices…' : 'Turning on alerts…');
        try {
            if (currentNotificationState.subscribed && currentNotificationState.subscription) {
                await saveNotificationPreferences(currentNotificationState.subscription, feeds, messageMode);
            } else {
                await enableNotifications(feeds, messageMode);
            }
            const state = await refreshNotificationControls();
            renderStatus(state);
            setMessage(
                messageMode === 'transcript'
                    ? 'Alerts will include a short transcript preview.'
                    : 'Alerts will show the feed name without a transcript.',
                'success',
            );
        } catch (error) {
            console.error('[Alerts] Could not save setup:', error);
            const state = await refreshNotificationControls();
            renderStatus(state);
            setMessage(error.message || 'Alerts could not be updated.', 'error');
        } finally {
            disableButton.disabled = false;
            updateSelectionSummary();
        }
    });

    disableButton.addEventListener('click', async () => {
        disableButton.disabled = true;
        primaryButton.disabled = true;
        setMessage('Turning off alerts…');
        try {
            await disableNotifications(currentNotificationState.subscription);
            const state = await refreshNotificationControls();
            renderStatus(state);
            setMessage('Alerts are off. Your feed choices stay saved on this device.', 'success');
        } catch (error) {
            console.error('[Alerts] Could not disable alerts:', error);
            setMessage('Alerts could not be turned off. Please try again.', 'error');
        } finally {
            disableButton.disabled = false;
            updateSelectionSummary();
        }
    });

    if (new URLSearchParams(window.location.search).get('notifications') === '1') {
        requestAnimationFrame(() => openDialog());
    }
}

function initPwaExperience() {
    const installButton = document.getElementById('install-btn');
    if (installButton && installButton.dataset.installBound !== '1') {
        installButton.dataset.installBound = '1';
        installButton.addEventListener('click', handleInstallButtonClick);
    }
    if (isStandalonePwa()) document.body.classList.add('pwa-standalone');
    setInstallButtonState(!!deferredInstallPrompt);
    initNotificationDialog();
    refreshNotificationControls();
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initPwaExperience);
} else {
    initPwaExperience();
}
window.addEventListener('load', initPwaExperience, { once: true });
