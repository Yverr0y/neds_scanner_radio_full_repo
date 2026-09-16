const CACHE_NAME = 'scanner-cache-v20260821-pwa-menu-r1';
const OFFLINE_URL = 'offline.html';

// Use relative paths so this worker works under /scanner/ when installed there.
const ASSETS_TO_CACHE = [
  './',
  OFFLINE_URL,
  'manifest.json',
  'static/icons/icon-192x192-v2.png',
  'static/icons/icon-512x512-v2.png',
  'static/icons/favicon.ico',
  'static/css/style.css',
  'static/css/tailwind.generated.css',

  // Add all critical JS/CSS (without ?v)
  'static/js/scanner_app_new.js',
  'static/js/scanner_player.js',
  'static/js/scanner_view.js',
  'static/js/scanner_archive.js',
  'static/js/scanner_heatmap.js',
  'static/js/scanner_incident.js',
  'static/js/scanner_neds_take.js',
  'static/js/scanner_review.js',
  'static/js/scanner_stats.js',
  'static/js/scanner_town.js',
  'static/js/pwa.js'
];


// Resilient precache: try to fetch each asset and only cache the ones that succeed.
self.addEventListener('install', (event) => {
  event.waitUntil((async () => {
    const cache = await caches.open(CACHE_NAME);
    const base = new URL('./', self.location);
    await Promise.all(
      ASSETS_TO_CACHE.map(async (asset) => {
        try {
          const url = new URL(asset, base).href;
          const res = await fetch(url, { cache: 'no-cache' });
          if (res && res.ok) {
            await cache.put(url, res.clone());
          } else {
            console.warn('SW: asset not cached (non-OK):', url, res && res.status);
          }
        } catch (err) {
          console.warn('SW: failed to fetch asset', asset, err);
        }
      })
    );
  })());
  self.skipWaiting();
});

// This is the ONLY fetch listener. It is the correct one.
self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;
  if (!event.request.url.startsWith('http')) return;

  // --- START: API Request Handling ---
  // If the request is for an API endpoint, always go to the network.
  // This ensures we get live data and that authentication cookies are sent.
  const isApiRequest = event.request.url.includes('/scanner/api/') ||
                       event.request.url.includes('/scanner/me') ||
                       event.request.url.includes('/scanner/logout');
  
  if (isApiRequest) { 
    // Do not cache API calls. Respond with the network request.
    event.respondWith(
      fetch(event.request)
    );
    return;
  }
  // --- END: API Request Handling ---

  // HTML navigations: network-first with offline fallback
  if (event.request.mode === 'navigate') {
    event.respondWith(
      fetch(event.request).catch(() => caches.match(new URL(OFFLINE_URL, self.location).href))
    );
    return;
  }

  // Everything else: network-first, cache-on-success
  event.respondWith(
    fetch(event.request)
      .then((response) => {
        if (response.status === 200) {
          const resClone = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, resClone));
        }
        return response;
      })
      .catch(() => caches.match(event.request))
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) => {
      return Promise.all(
        keys.map((key) => {
          // Delete ALL old caches (different name), and also purge icon/manifest
          // entries from the current cache so they re-fetch with the new logo
          if (key !== CACHE_NAME) {
            return caches.delete(key);
          } else {
            return caches.open(key).then((cache) =>
              cache.keys().then((requests) =>
                Promise.all(
                  requests
                    .filter((r) => r.url.includes('manifest.json') || r.url.includes('/icons/'))
                    .map((r) => cache.delete(r))
                )
              )
            );
          }
        })
      );
    })
  );
  self.clients.claim();
});


// Listen for messages from the page (e.g., SKIP_WAITING)
self.addEventListener('message', (event) => {
  if (!event.data) return;
  if (event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});

//
// THE SECOND, BROKEN 'fetch' LISTENER HAS BEEN DELETED
//


// Handle push events with one concise, deep-linked notification per feed.
self.addEventListener('push', (event) => {
  let payload = {};
  try {
    if (event.data) payload = event.data.json();
  } catch (e) {
    try { payload = { message: event.data.text() }; } catch (e2) { payload = { message: 'New notification' }; }
  }

  const feed = payload.feed || payload.data?.feed || '';
  const targetUrl = payload.data?.url || (feed ? `/scanner/view?feed=${encodeURIComponent(feed)}` : '/scanner/');
  const title = payload.title || 'Scanner activity';
  const options = {
    body: payload.message || 'A new scanner call is ready to listen.',
    icon: '/scanner/static/icons/icon-192x192-v2.png',
    badge: '/scanner/static/icons/icon-192x192-v2.png',
    tag: payload.tag || (feed ? `scanner-call-${feed}` : 'scanner-activity'),
    renotify: true,
    timestamp: Date.now(),
    data: { ...(payload.data || {}), url: targetUrl, feed },
    actions: [
      { action: 'listen', title: 'Listen now' },
      { action: 'archive', title: 'Open archive' },
    ],
  };

  event.waitUntil(self.registration.showNotification(title, options));
});


// Handle notification click
self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const feed = event.notification.data?.feed || '';
  const targetUrl = event.action === 'archive'
    ? `/scanner/archive${feed ? `?feed=${encodeURIComponent(feed)}` : ''}`
    : (event.notification.data?.url || '/scanner/');
  const urlToOpen = new URL(targetUrl, self.location.origin).href;
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then((windowClients) => {
      for (const client of windowClients) {
        if (client.url === urlToOpen && 'focus' in client) return client.focus();
      }
      for (const client of windowClients) {
        if ('navigate' in client && 'focus' in client) {
          return client.navigate(urlToOpen).then(() => client.focus());
        }
      }
      if (clients.openWindow) return clients.openWindow(urlToOpen);
    })
  );
});
