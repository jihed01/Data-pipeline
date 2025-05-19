import sys
from datetime import date, timedelta
import numpy as np
from typing import Optional
import logging


class AdvancedVisitSensor:
    """
    Capteur de visiteurs avancé avec :
    - Génération de NaN, outliers positifs/négatifs
    - Logging des anomalies
    - Variations hebdomadaires des taux d'erreur
    - Configuration flexible
    """

    def __init__(
            self,
            avg_visit: int = 1500,
            std_visit: int = 150,
            base_perc_nan: float = 0.02,
            base_perc_outlier: float = 0.01,
            outlier_multiplier: int = 10,
            negative_outlier_ratio: float = 0.3,  # 30% des outliers sont négatifs
            weekend_error_boost: float = 2.0,  # 2x plus d'erreurs le weekend
    ):
        self.avg_visit = avg_visit
        self.std_visit = std_visit
        self.base_perc_nan = base_perc_nan
        self.base_perc_outlier = base_perc_outlier
        self.outlier_multiplier = outlier_multiplier
        self.negative_outlier_ratio = negative_outlier_ratio
        self.weekend_error_boost = weekend_error_boost

        # Configuration du logging
        logging.basicConfig(
            filename='sensor_anomalies.log',
            level=logging.INFO,
            format='%(asctime)s - %(message)s'
        )
        self.logger = logging.getLogger('VisitSensor')

    def _get_error_probabilities(self, day_of_week: int) -> tuple:
        """Adapte les probabilités d'erreur selon le jour de la semaine"""
        base_multiplier = self.weekend_error_boost if day_of_week >= 5 else 1.0
        return (
            min(self.base_perc_nan * base_multiplier, 0.5),
            min(self.base_perc_outlier * base_multiplier, 0.5)
        )

    def simulate_visits(self, business_date: date) -> Optional[float]:
        """Simule le nombre de visiteurs avec anomalies contrôlées"""
        np.random.seed(seed=business_date.toordinal())
        day_of_week = business_date.weekday()

        # 1. Gestion jours fermés (dimanche)
        if day_of_week == 6:
            return -1

        # 2. Calcul des probabilités d'erreur adaptatives
        perc_nan, perc_outlier = self._get_error_probabilities(day_of_week)

        # 3. Génération de NaN (avec probabilité adaptée)
        if np.random.random() < perc_nan:
            self.logger.warning(f"NaN generated for {business_date}")
            return np.nan

        # 4. Génération de la valeur de base
        visit = np.random.normal(self.avg_visit, self.std_visit)

        # 5. Application des variations journalières
        weekday_factors = {2: 1.10, 4: 1.25, 5: 1.35}  # Mercredi, Vendredi, Samedi
        visit *= weekday_factors.get(day_of_week, 1.0)

        # 6. Génération d'outliers (positifs ou négatifs)
        if np.random.random() < perc_outlier:
            if np.random.random() < self.negative_outlier_ratio:
                visit = -abs(visit) * self.outlier_multiplier
                self.logger.error(f"Negative outlier generated: {visit} for {business_date}")
            else:
                visit *= self.outlier_multiplier
                self.logger.error(f"Positive outlier generated: {visit} for {business_date}")

        return np.floor(visit)

    def get_visit_count(self, business_date: date) -> Optional[float]:
        """Point d'entrée principal avec gestion des erreurs"""
        try:
            np.random.seed(seed=business_date.toordinal() + 1)
            result = self.simulate_visits(business_date)

            # Conversion explicite des -1 en None pour les jours fermés
            return None if result == -1 else result

        except Exception as e:
            self.logger.critical(f"Critical failure for {business_date}: {str(e)}")
            return np.nan


# Exemple d'utilisation avec visualisation des anomalies
if __name__ == "__main__":
    print("=== Test du capteur avancé (30 jours) ===")
    sensor = AdvancedVisitSensor(
        base_perc_nan=0.04,
        base_perc_outlier=0.02,
        negative_outlier_ratio=0.4,
        weekend_error_boost=2
    )

    test_date = date(2024, 1, 1)
    for _ in range(30):
        count = sensor.get_visit_count(test_date)

        # Affichage avec codes couleur
        if count is None:
            status = "🟦 Fermé"
        elif np.isnan(count):
            status = "🟪 NaN"
        elif count < 0:
            status = f"🔴 Outlier négatif: {count}"
        elif count > 10000:
            status = f"🟡 Outlier positif: {count}"
        else:
            status = f"🟢 Normal: {count}"

        print(f"{test_date}: {status}")
        test_date += timedelta(days=1)