#!/usr/bin/env python3
"""
Script pour traiter toutes les entreprises en batch automatiquement.
Gère les reprises automatiques et les erreurs de connexion.
"""

import subprocess
import sys
import time
import os

def run_batch(offset, limit, headed=True, dry_run=False):
    """Lance une batch d'enrichissement."""
    cmd = [
        sys.executable, "google_reviews_worker.py",
        f"--offset", str(offset),
        f"--limit", str(limit),
    ]

    if headed:
        cmd.append("--headed")
    if dry_run:
        cmd.append("--dry-run")

    print(f"\n{'='*70}")
    print(f"  BATCH {offset//limit + 1} | Offset: {offset} | Limite: {limit}")
    print(f"{'='*70}\n")

    try:
        result = subprocess.run(cmd, cwd=os.path.dirname(os.path.abspath(__file__)))
        return result.returncode == 0
    except KeyboardInterrupt:
        print("\n⛔ Arrêt par l'utilisateur (Ctrl+C)")
        return False
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        return False


def main():
    import argparse

    p = argparse.ArgumentParser(
        description="Traitement batch automatique de toutes les entreprises"
    )
    p.add_argument("--batch-size", type=int, default=100,
                   help="Nombre d'entreprises par batch (défaut: 100)")
    p.add_argument("--max-batches", type=int, default=None,
                   help="Nombre max de batches (défaut: traiter toutes)")
    p.add_argument("--start-offset", type=int, default=0,
                   help="Offset de départ (pour reprendre) (défaut: 0)")
    p.add_argument("--dry-run", action="store_true",
                   help="Mode dry-run (ne pas écrire en DB)")
    p.add_argument("--headless", action="store_true",
                   help="Mode headless (pas de browser visible)")
    p.add_argument("--delay", type=int, default=5,
                   help="Délai en secondes entre les batches (défaut: 5)")

    args = p.parse_args()

    offset = args.start_offset
    batch_num = offset // args.batch_size
    batches_completed = 0

    print("\n" + "="*70)
    print("  ENRICHISSEMENT BTP — TRAITEMENT BATCH AUTOMATIQUE")
    print(f"  Taille batch     : {args.batch_size}")
    print(f"  Offset départ    : {args.offset if hasattr(args, 'offset') else offset}")
    print(f"  Max batches      : {args.max_batches or 'Illimité'}")
    print(f"  Mode             : {'DRY-RUN' if args.dry_run else 'Production'}")
    print(f"  Délai entre lots : {args.delay}s")
    print("="*70 + "\n")

    while True:
        # Vérifier la limite de batches
        if args.max_batches and batches_completed >= args.max_batches:
            print(f"\n✅ Limite de {args.max_batches} batches atteinte.")
            break

        # Lancer la batch
        success = run_batch(
            offset=offset,
            limit=args.batch_size,
            headed=not args.headless,
            dry_run=args.dry_run
        )

        if not success:
            print(f"\n⚠️  Batch échouée. Reprendre avec:")
            print(f"   python batch_enrich.py --start-offset {offset} ...")
            sys.exit(1)

        # Préparer la prochaine batch
        offset += args.batch_size
        batches_completed += 1

        # Demander si on continue
        if args.max_batches is None:  # Si pas de limite, demander
            try:
                response = input(f"\n⏸️  Continuer avec la batch suivante? (y/n/s pour sauter) [y]: ").strip().lower()
                if response == 'n':
                    print(f"\n✅ Arrêt. Reprendre avec:")
                    print(f"   python batch_enrich.py --start-offset {offset} ...")
                    break
                elif response == 's':
                    print(f"\n⏭️  Batch suivante skippée.")
                    offset += args.batch_size
                    continue
            except KeyboardInterrupt:
                print(f"\n⛔ Arrêt (Ctrl+C). Reprendre avec:")
                print(f"   python batch_enrich.py --start-offset {offset} ...")
                break

        # Délai avant la prochaine batch
        if args.delay > 0:
            print(f"\n⏳ Attente {args.delay}s avant la prochaine batch...")
            time.sleep(args.delay)

    print(f"\n✅ Traitement complété ({batches_completed} batches).")


if __name__ == "__main__":
    main()
