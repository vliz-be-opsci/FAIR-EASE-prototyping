Prototype/POC of the FAIR-EASE DAL (Discovery System).

The tripleStore URL is defined in the `config.yaml`

Based on the `D4.1` `Table 1`, 

- Can search for datasets which contains :
    - Spatial coverage (`latitude.min` and/or `latitude.max`, and/or, `longitude.min` and/or `longitude.max`), not possible to filter with `z` or `depth`
    - Time coverage (`time.min` and/or `time.max`)
    - Keywords
    - Eurobis Dataset RDF referer
    - Eurobis Column RDF referer
    - Word(s) in dataset Title
    - Word(s) in dataset License
    - Word(s) in dataset Description
    - Dataset Creation Date
    - Output file format(s)
    - `mail` and/or `url` and/or `mail` box of the dataset's Creator/Publisher/Contact Point

- Get the basic Datasets URL of each matching datasets
- Get the placeholdered Template of each matching datasets
- Get the URL of the (full) dataset file-format
- Get the URL of the (optimised (Subsetting)) dataset file-format
- Get the pandas.Dataframe of each dataset URL (if the file-format is `.json` or `.csv`, `.nc` doesn't work)
- Get the merged pandas.Dataframe of dataset URL (if the file-format is `.json` or `.csv`, `.nc` doesn't work) (absolutely not optimal, merged just the different pandas.Dataframe, does not reindex commons values (latitude/longitude..))
