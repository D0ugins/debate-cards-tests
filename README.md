Works on Python 3.11, probably on slightly ealier versions as well

Packages can be installed with `conda env create -f evironment.yml`

Data can be loaded from a postgres server running at the url at the top of `split.ipynb`. Setting `BUCKET_ID` to a value that already exists in the `./data` folder lets you test data without a database set up
