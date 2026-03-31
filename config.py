"""
Shared constants for the Promo ROI Engine pipeline.
Imported by 01_generate_dataset.py and 03_generate_daily.py.
"""

CHANNELS = {
    "organic_search": {"weight": 0.28, "base_cvr": 0.032, "elasticity": -0.9},
    "paid_search":    {"weight": 0.22, "base_cvr": 0.058, "elasticity": -1.4},
    "email":          {"weight": 0.18, "base_cvr": 0.071, "elasticity": -2.1},
    "social_organic": {"weight": 0.14, "base_cvr": 0.021, "elasticity": -0.7},
    "paid_social":    {"weight": 0.10, "base_cvr": 0.044, "elasticity": -1.6},
    "direct":         {"weight": 0.05, "base_cvr": 0.039, "elasticity": -1.0},
    "referral":       {"weight": 0.03, "base_cvr": 0.027, "elasticity": -0.8},
}

PRODUCTS = {
    "analytics_pro":   {"base_price": 199.0, "category": "software"},
    "data_starter":    {"base_price":  49.0, "category": "software"},
    "consulting_hour": {"base_price": 150.0, "category": "services"},
    "training_bundle": {"base_price":  89.0, "category": "education"},
    "api_access":      {"base_price":  29.0, "category": "software"},
}

DISCOUNT_TIERS = [0.0, 0.0, 0.0, 0.05, 0.10, 0.10, 0.15, 0.20, 0.25]

PAID_CHANNELS = ["paid_search", "email", "paid_social"]
