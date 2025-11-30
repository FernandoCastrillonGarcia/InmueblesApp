from functools import partial

from sklearn.model_selection import cross_val_score
from sklearn.metrics import make_scorer
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import ElasticNet
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor

import optuna

from evaluate import median_absolute_percentage_error

# Suppress Optuna's info messages
optuna.logging.set_verbosity(optuna.logging.WARNING)

# Hyperparameters
def optimize_hyperparameters(objective_func, X_train, y_train):
    study = optuna.create_study(direction='minimize', sampler=optuna.samplers.RandomSampler(seed=42)) # Default is random Search

    objective = partial(objective_func, X=X_train, y = y_train)
    
    # Use it like this:
    study.optimize(objective, n_trials=50, n_jobs=-1, show_progress_bar=True)

    best_params = study.best_params
    return best_params