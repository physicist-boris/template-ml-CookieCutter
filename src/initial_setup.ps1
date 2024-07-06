conda create -n "bxc_anonymisation" python=3.9
conda activate bxc_anonymisation
pip install -r ./env/requirements-dev.txt
git init
pre-commit install
$env:PYTHONPATH = $pwd
$env:MLFLOW_TRACKING_URI = "databricks"
mlflow experiments create --experiment-name "/Users/louis.bourassa-ext@ramq.gouv.qc.ca/BxC-Anonymisation"