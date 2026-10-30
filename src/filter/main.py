"""Punto de entrada de las entidades filter.

La entidad a ejecutar se elige con ``FILTER_ROLE`` (por ejemplo ``q1``). Asi una
misma imagen Docker puede desplegarse como cualquier filtro cambiando solo las
variables de entorno.
"""

import logging
import os

from filter.year_language import YearLanguageFilter

FILTERS = {
    "q1": YearLanguageFilter,
}


def main():
    logging.basicConfig(level=logging.INFO)

    role = os.environ.get("FILTER_ROLE", "").strip().lower()
    if role not in FILTERS:
        raise SystemExit(
            f"FILTER_ROLE must be one of {sorted(FILTERS)}, got {role!r}"
        )

    FILTERS[role]().start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
