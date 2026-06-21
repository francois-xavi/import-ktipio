"""
Watchdog interne anti-gel.

Problème résolu : un worker peut se figer (process vivant mais bloqué dans un
appel Playwright/réseau qui ne rend jamais la main). Dans ce cas Docker ne le
voit pas planté → `restart: unless-stopped` ne le relance JAMAIS. C'est ce qui a
causé la panne silencieuse de 5 jours.

Fonctionnement : un thread démon surveille un horodatage de "dernier progrès".
Le worker appelle mark_progress() à chaque itération de boucle et après chaque
entreprise traitée. Si plus aucun progrès pendant WATCHDOG_TIMEOUT secondes,
le watchdog tue le process (os._exit) → Docker le relance automatiquement.

Important : pendant l'attente légitime "rien à l'offset, retry dans 60s", le
worker continue d'appeler mark_progress() à chaque itération, donc le watchdog
ne se déclenche PAS. Il ne tue que les vrais gels (bloqué > timeout en plein
traitement).

Réglage via env var WATCHDOG_TIMEOUT (secondes, défaut 600 = 10 min).
"""
import os
import time
import threading
import logging

log = logging.getLogger("watchdog")

_last_progress = time.time()
_lock = threading.Lock()


def mark_progress() -> None:
    """À appeler quand le worker avance (itération de boucle, entreprise traitée)."""
    global _last_progress
    with _lock:
        _last_progress = time.time()


def start_watchdog(timeout: int | None = None, label: str = "") -> None:
    """Démarre le thread de surveillance. À appeler une fois avant la boucle."""
    timeout = timeout or int(os.getenv("WATCHDOG_TIMEOUT", "600"))
    suffix = f" [{label}]" if label else ""

    def _loop():
        while True:
            time.sleep(30)
            with _lock:
                idle = time.time() - _last_progress
            if idle > timeout:
                log.error(
                    f"🛑 Watchdog{suffix}: aucun progrès depuis {idle:.0f}s "
                    f"(>{timeout}s) — worker gelé, redémarrage forcé du process."
                )
                os._exit(1)

    threading.Thread(target=_loop, daemon=True, name="watchdog").start()
    log.info(f"🐕 Watchdog actif{suffix} (timeout={timeout}s)")
