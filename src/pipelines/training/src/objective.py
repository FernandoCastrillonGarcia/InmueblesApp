from sklearn.model_selection import cross_val_score
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import make_scorer

from evaluate import median_absolute_percentage_error

# Objective functions 
def objective_random_forest(trial, X = None, y = None):

    n_estimators = trial.suggest_int('n_estimators', 100, 1000)
    max_depth = trial.suggest_int('max_depth', 10, 50)
    min_samples_split = trial.suggest_int('min_samples_split', 2, 32)
    min_samples_leaf = trial.suggest_int('min_samples_leaf', 1, 32)

    model = RandomForestRegressor(n_estimators=n_estimators,
    max_depth=max_depth,
    min_samples_split=min_samples_split,
    min_samples_leaf=min_samples_leaf)

    score = cross_val_score(model, X, y, n_jobs=-1, cv=5, scoring=make_scorer(median_absolute_percentage_error)).mean()

    return score

def objective_xgboost(trial, X = None, y = None):
     
    params = {
        'n_estimators': trial.suggest_int('n_estimators', 100, 1000),
        'max_depth': trial.suggest_int('max_depth', 3, 12),
        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
        'subsample': trial.suggest_float('subsample', 0.6, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
        'gamma': trial.suggest_float('gamma', 0, 5),
        'min_child_weight': trial.suggest_int('min_child_weight', 1, 10),
        'reg_alpha': trial.suggest_float('reg_alpha', 0, 1),
        'reg_lambda': trial.suggest_float('reg_lambda', 0, 1),
        'random_state': 42,
        'n_jobs': -1
    }
        
    
    model = XGBRegressor(**params)

    score = cross_val_score(model, X, y.ravel(), cv=5, 
                               scoring=make_scorer(median_absolute_percentage_error), n_jobs=1).mean()

    return score

def objective_lightgbm(trial, X = None, y = None):
    params = {
        'n_estimators': trial.suggest_int('n_estimators', 100, 800),
        'max_depth': trial.suggest_int('max_depth', 3, 12),
        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
        'num_leaves': trial.suggest_int('num_leaves', 20, 200),
        'feature_fraction': trial.suggest_float('feature_fraction', 0.7, 1.0),
        'bagging_fraction': trial.suggest_float('bagging_fraction', 0.7, 1.0),
        'bagging_freq': trial.suggest_int('bagging_freq', 1, 5),
        'min_child_samples': trial.suggest_int('min_child_samples', 10, 50),
        'reg_alpha': trial.suggest_float('reg_alpha', 0, 0.1),
        'reg_lambda': trial.suggest_float('reg_lambda', 0, 0.1),
        'random_state': 42,
        'verbose': -1
        }
        
    model = LGBMRegressor(**params)

    score = cross_val_score(model, X, y.ravel(), cv=5, 
                           scoring=make_scorer(median_absolute_percentage_error), n_jobs=1).mean()
    return score

def objective_elastic_net(trial, X=None, y=None):
    params = {
        'alpha': trial.suggest_float('alpha', 1e-4, 10, log=True),
        'l1_ratio': trial.suggest_float('l1_ratio', 0.1, 0.9),
        'max_iter': trial.suggest_int('max_iter', 1000, 3000),
        'tol': trial.suggest_float('tol', 1e-5, 1e-3, log=True),
        'random_state': 42
    }

    model = ElasticNet(**params)

    score = cross_val_score(model, X, y.ravel(), cv=5, 
                           scoring=make_scorer(median_absolute_percentage_error), n_jobs=1).mean()
    return score