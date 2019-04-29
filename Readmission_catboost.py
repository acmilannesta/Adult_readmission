import os,  pandas as pd,  numpy as np
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import StratifiedKFold
from catboost import CatBoostClassifier
from hyperopt import fmin, hp, tpe, STATUS_OK, Trials

analysis = pd.read_sas('Dataset//combined_vars.sas7bdat', encoding='latin1').drop(['PERSON_ID', 'TRR_ID'], axis=1)

X = analysis.drop('CODE_REHOSP', 1)
y = analysis['CODE_REHOSP'].replace(2, 0)

cat_colidx = [X.columns.get_loc(col) for col in X.columns if X[col].nunique() <= 10]

for col in cat_colidx:
    if X[X.columns[col]].dtype == 'float64':
        X[X.columns[col]] = X[X.columns[col]].fillna(-1).astype(int)
    else:
        X[X.columns[col]] = X[X.columns[col]].fillna('')

cbc_params = {
    'max_depth': hp.choice('max_depth', np.arange(2, 11)),
    'l2_leaf_reg': hp.uniform('l2_leaf_reg', 0, 100),
    'colsample_bylevel': hp.uniform('colsample_bylevel', 0.1, 1),
    'subsample': hp.uniform('subsample', 0.1, 1),
    'eta': hp.uniform('eta', 0.01, 0.1)
}

def f_cbc(params):
    kfold = StratifiedKFold(5, True, 2019)
    auc = np.zeros(kfold.get_n_splits())
    cbc_pred = np.zeros(len(X))
    for i, (tr_idx, val_idx) in enumerate(kfold.split(X, y)):
        cbc = CatBoostClassifier(
            **params,
            n_estimators=2000,
            random_state=2019,
            eval_metric='AUC',
            cat_features=cat_colidx,
            silent=True,
            one_hot_max_size=2,
            bootstrap_type='Bernoulli',
            boosting_type='Plain',
        )
        clf = cbc.fit(X.iloc[tr_idx], y[tr_idx], use_best_model=True,
                       eval_set=[(X.iloc[tr_idx], y[tr_idx]), (X.iloc[val_idx], y[val_idx])],
                       early_stopping_rounds=100,
                       verbose_eval=False)
        cbc_pred[val_idx] = clf.predict_proba(X.iloc[val_idx])[:, 1]
        auc[i] = roc_auc_score(y[val_idx], cbc_pred[val_idx])
        # print("Mean AUC(%g|%g): %.5f" %(i, kfold.get_n_splits(), np.sum(auc)/i))
    return {'loss': -np.mean(auc).round(5), 'status': STATUS_OK}
trials = Trials()
cbc_best = fmin(f_cbc, cbc_params, algo=tpe.suggest, rstate=np.random.RandomState(1565), max_evals=50, trials=trials)