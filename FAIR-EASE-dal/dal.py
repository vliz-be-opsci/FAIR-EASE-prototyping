from pathlib import Path
import pandas as pd
import xarray as xr
from pykg2tbl import DefaultSparqlBuilder, KGSource
from typing import List
import regex as re
import yaml

CONF_FILE_PATH = 'FAIR-EASE-dal/config.yaml'


class UDAL :
    GENERATOR = DefaultSparqlBuilder(templates_folder=str(Path().absolute())+'/FAIR-EASE-dal/templatedQueries/')
    KG               : KGSource
    TSUrl            : str            = ''
    provUrl          : [str]          = []
    provBasicUrl     : [str]          = []
    provOptimalUrl   : [str]          = []
    associatedFormat : [str]          = []
    datasetsDf       : [pd.DataFrame] = []
    datasetsMergedD  : pd.DataFrame   = []
    params           : dict           = {'variables' : []}


    def __init__(self):
        self.loadSourceConfig()


    def loadSourceConfig(self, sourceIdx : str = 0):
        with open(CONF_FILE_PATH, 'r') as f:
            valConf = yaml.safe_load(f)
            if 'sources' not in valConf or not len(valConf):
                return

            self.TSUrl = valConf['sources'][sourceIdx]['baseUrl']
            self.KG    = KGSource.build(self.TSUrl)


    def setParams(self, **vars):
        notOverWrite = ('title', 'license', 'description', 'format', 'keywords', 'eurobisDataset', 'eurobisColumn', 'variables')

        for var in vars:
            if var not in self.params or var not in notOverWrite:
                self.params[var] = [vars[var]] if type(vars[var]) == str else vars[var]
                continue

            vVar = self.params[var]
            if type(vars[var]) is list:     # is a list, add/append to the existing value
                for idxSubVar in range(0, len(vars[var])):
                    if type(vVar) is list:
                        vVar.append(vars[var][idxSubVar])
                    else:
                        vVar = [vVar]
                        vVar.append(vars[var][idxSubVar])

            elif type(vars[var]) is tuple:  # is a tuple, overwrite the existing value
                vVar = []
                for idxSubVar in range(0, len(vars[var])):
                    vVar = vars[var][idxSubVar]

            else:                               # is anything else, add/append to the existing value                
                if type(vVar) is list:
                    vVar.append(vars[var])
                else:
                    vVar = [vVar]   
                    vVar.append(vars[var])

        for var in self.params:
            vVar = self.params[var]

            if vVar not in notOverWrite and type(vVar) is list and len(vVar) >= 2:
                maxIdx = 2 if len(vVar) > 2 else 1
                if ((str(vVar[0]).isdigit() or str(vVar[maxIdx]).isdigit()) and float(vVar[0]) > float(vVar[maxIdx])) \
                    or str(vVar[0]) > str(vVar[maxIdx]):
                    vVar[maxIdx], vVar[0] = vVar[0], vVar[maxIdx]


    def getWktLitteral(self, latitude : List[str] = [], longitude : List[str] = []) -> str:
        if not len(latitude):
            latitude = [-90, 90]
        if not len(longitude):
            longitude = [-180, 180]

        lLatitude = len(latitude)
        lLongitude = len(longitude)
        varFor = 0

        if latitude[0] == latitude[1] or len(latitude) == 1:
             lLatitude = 1
        if longitude[0] == longitude[1] or len(latitude) == 1:
            lLongitude = 1
            if lLatitude == 2:
                 varFor=1

        nLocation = lLatitude * lLongitude
        sLatLong = 'POINT (' if lLatitude == 1 and lLongitude == 1 else 'LineString ((' if lLatitude == 1 or lLongitude == 1 else 'POLYGON (('

        if lLatitude == 2:
            nLocation += 1

        while varFor < nLocation :
            sLatLong += '{0} {1}{2}'.format(longitude[((varFor+1)>>1)%2], latitude[(varFor>>1)%2], '' if varFor == nLocation-1 else ',')
            varFor += 1
        sLatLong += '))' if nLocation > 1 else ')'

        return sLatLong


    def getResult(self, queryFileName: str, KG: KGSource, **vars) -> pd.DataFrame:
        sparql = self.GENERATOR.build_syntax(queryFileName, **vars)
        return KG.query(sparql=sparql).to_dataframe()


    def findMatchingDataset(self, **params) -> list[str]:
        self.setParams(**params)
        bbox = ''
        if 'latitude' in params or 'longitude' in params:
            bbox = self.getWktLitteral(
                params['latitude']  if 'latitude'  in params else [],
                params['longitude'] if 'longitude' in params else []
            )

        self.provUrl = self.getResult(queryFileName='findDatasets.sparql', KG=self.KG, bbox=bbox, **params)['datasetURL'].to_list()

        if len(self.provUrl) > 0:
            return self.provUrl
        return []


    def getBasicPatterns(self, datasetsUrl = []) -> list[str]:

        if not len(datasetsUrl) and len(self.provUrl):
            datasetsUrl = self.provUrl

        if not len(datasetsUrl):
            return ['No dataset found']

        patternList = []
        for datasetUrl in datasetsUrl:
            result = self.getResult(queryFileName='findPattern.sparql', KG=self.KG, datasetURL=datasetUrl)
            patternList.append(result['pattern'].to_list()[0])
        return patternList


    def getBasicDatasetUrl(self, datasetsUrl = [], **vars) -> list[str]:
        self.setParams(**vars)

        if not len(datasetsUrl) and len(self.provUrl):
            datasetsUrl = self.provUrl

        if not len(datasetsUrl):
            return ['No dataset Found']

        self.provBasicUrl  = []
        self.associatedFormat = []

        for datasetUrl in datasetsUrl:
            print('******************')
            result = self.getResult(queryFileName='findPattern.sparql', KG=self.KG, datasetURL=datasetUrl)
            pattern = result['pattern'].to_list()[0]

            if 'format' in self.params and len(self.params['format']):
                for format in self.params['format'] :
                    formatRes = self.getResult(queryFileName='findAvailableFormat.sparql', KG=self.KG, datasetURL=datasetUrl ,format=format)
                    if len(formatRes) > 0:
                        pattern = pattern.replace(r'{format}', format)
                        self.associatedFormat.append(format)
                        break
            else :
                self.associatedFormat.append('csv')

            for idx, row in result.iterrows():
                matches = re.findall(r'{{\W*?' + row['varName'] + r'.*?}?}}', pattern)
                for match in matches:
                    pattern = pattern.replace(match, '')

            pos = str(pattern).find('?')
            if len(pattern) > pos+1 and str(pattern[pos+1]) in [',', '&', '|', '?']:
                pattern = pattern[0:pos+1] + pattern[pos+2:len(pattern)]
            if pattern[len(pattern)-1] in [',', '&', '|', '?']:
                pattern = pattern[0:len(pattern)-1]
            print(pattern)
            self.provBasicUrl.append(pattern)

        return self.provBasicUrl


    def getOptimalDatasetUrl(self, datasetsUrl = [], **vars) -> list[str]:
        self.setParams(**vars)

        if not len(datasetsUrl) and len(self.provUrl):
            datasetsUrl = self.provUrl

        if not len(datasetsUrl):
            return ['No dataset Found']

        self.provOptimalUrl = []
        self.associatedFormat  = []

        for datasetUrl in datasetsUrl:
            print('******************')
            result = self.getResult(queryFileName='findPattern.sparql', KG=self.KG, datasetURL=datasetUrl)
            pattern = result['pattern'].to_list()[0]

            if 'format' in self.params and len(self.params['format']):
                for format in self.params['format'] :
                    formatRes = self.getResult(queryFileName='findAvailableFormat.sparql', KG=self.KG, datasetURL=datasetUrl ,format=format)
                    if len(formatRes) > 0:
                        pattern = pattern.replace(r'{format}', format)
                        self.associatedFormat.append(format)
                        break

            for idx, row in result.iterrows():
                isRequired = True if row['required'] == 'true' else False
                varName = row['varName']
                value   = row['defaultValue']

                if '.' in varName and varName.split('.')[0] in self.params:

                    vName  = varName.split('.')[0]
                    varAdd = varName.split('.')[1]

                    if varAdd == 'min' and len(self.params[vName]) > 0 :
                        if (str(self.params[vName][0]).isdigit() or str(value).isdigit()) and float(self.params[vName][0]) > float(value):
                            value = self.params[vName][0]
                        elif str(self.params[vName][0]) > str(value):
                            value = self.params[vName][0]

                    elif varAdd == 'str' and len(self.params[vName]) > 2:
                        if (str(self.params[vName][1]).isdigit() or str(value).isdigit()) and float(self.params[vName][0]) > float(value):
                            value = self.params[vName][1]
                        elif str(self.params[vName][1]) > str(value):
                            value = self.params[vName][1]
    
                    elif varAdd == 'max' and len(self.params[vName]) > 1:
                        idx = 2 if len(self.params[vName]) > 2 else 1 
                        if (str(self.params[vName][idx]).isdigit() or str(value).isdigit()) and float(self.params[vName][idx]) < float(value):
                            value = self.params[vName][idx]
                        elif str(self.params[vName][idx]) < str(value):
                            value = self.params[vName][idx]

                elif varName != value and varName in self.params:
                    value = self.params[varName][0] if type(self.params[varName]) is list and len(self.params[varName]) > 0 else self.params[varName]

                if (varName != value) or isRequired:
                    pattern = pattern.replace('{{{0}}}'.format(varName), str(value))            
                else :
                    if varName in self.params or ('variables' in self.params and varName in self.params['variables']): # is in params query
                        matches = re.findall(r'{{\W*?' + varName + r'.*?}?}}', pattern)
                        for match in matches:
                            newV = match.replace('{{', '').replace('}}', '')
                            pattern = pattern.replace(match, newV)

                    else : # is not in params query
                        matches = re.findall(r'{{\W*?' + varName + r'.*?}?}}', pattern)
                        for match in matches:
                            pattern = pattern.replace(match, '')

            pos = str(pattern).find('?')
            if len(pattern) > pos+1 and str(pattern[pos+1]) in [',', '&', '|', '?']:
                pattern = pattern[0:pos+1] + pattern[pos+2:len(pattern)]
            if pattern[len(pattern)-1] in [',', '&', '|', '?']:
                pattern = pattern[0:len(pattern)-1]
            print(pattern)
            self.provOptimalUrl.append(pattern)

        return self.provOptimalUrl


    def downloadDataset(self, datasetUrl: str, format: str) -> pd.DataFrame:
        # need to find other {format} => pandas.Dataframe
        match format:
            case 'csv':
                try :
                    return pd.read_csv(datasetUrl)
                except: 
                    return pd.DataFrame()
            case 'json':
                try :
                    return pd.read_json(datasetUrl)
                except: 
                    return pd.DataFrame()
            case 'nc':
                # does not seems to work
                try:
                    return xr.open_dataset(datasetUrl).to_dataframe()
                except:
                    return pd.DataFrame()
            case _:
                    return pd.DataFrame()


    def getDownloadDatasets(self, datasetsUrl = []) -> pd.DataFrame:

        if not len(datasetsUrl) and len(self.provOptimalUrl):
            datasetsUrl = self.provOptimalUrl
        elif not len(datasetsUrl) and len(self.provUrl):
            datasetsUrl = self.provUrl

        if not len(datasetsUrl):
            return ['No dataset Found']

        self.datasetsDataframe = []
        for idxDataset in range(0, len(datasetsUrl)):
            dataframe = self.downloadDataset(datasetUrl=datasetsUrl[idxDataset], format=self.associatedFormat[idxDataset])
            self.datasetsDataframe.append(dataframe)

        return self.datasetsDataframe


    def getDownloadMergedDatasets(self, datasetsUrl = []) -> pd.DataFrame:

        if not len(datasetsUrl) and len(self.provOptimalUrl):
            datasetsUrl = self.provOptimalUrl
        elif not len(datasetsUrl) and len(self.provUrl):
            datasetsUrl = self.provUrl

        if not len(datasetsUrl):
            return ['No dataset Found']

        self.datasetsDataframe = []
        mergedDataframe = pd.DataFrame()

        for idxDataset in range(0, len(datasetsUrl)):
            dataframe = self.downloadDataset(datasetUrl=datasetsUrl[idxDataset], format=self.associatedFormat[idxDataset])
            self.datasetsDataframe.append(dataframe)
            mergedDataframe = pd.concat([mergedDataframe, dataframe], ignore_index=True, sort=False)
        return self.datasetsDataframe



if __name__ == '__main__':
    # params: dict = dict(bbox = 'POINT(51.6 3.95)')
    # params: dict = dict(minDate = '2023-01-01T05:30:20Z', maxDate = '2023-01-01T05:30:20Z')
    # params: dict = dict(keywords=['temperature', 'date'])
    # params: dict = dict(eurobisDataset=['https://vocab.nerc.ac.uk/collection/P02/current/BPRP/'])
    # params: dict = dict(eurobisColumn=['https://vocab.nerc.ac.uk/collection/P01/current/SNANID01/', 'https://vocab.nerc.ac.uk/collection/P01/current/SCNAME01/'])
    # params: dict = dict(inTitle=['LOcal', 'study'])
    # params: dict = dict(inLicense=['https://spdx.org/licenses/0BSD.html', 'redistributed'])
    # params: dict = dict(inDescription=['Hazards', 'Sea Surface'])
    # params: dict = dict(minDatasetCreationDate='2020-04-22T22:30:00Z', maxDatasetCreationDate='2020-04-22T22:55:00Z')
    # params: dict = dict(format=['json', 'csv'])
    # params: dict = dict(involvedList=['creator', 'publisher'])
    # params: dict = dict(involvedList=['creator'])
    # params: dict = dict(involvedList=['creator'], involvedName= ['Pete'])
    # params: dict = dict(involvedList=['creator'], involvedUrl = ['https://www.chc.ucsb.edu/data/chirps'])
    # params: dict = dict(involvedList=['creator'], involvedMail= ['pete'])
    # params: dict = dict(bbox = 'POINT(51.6 3.95)', minDate = '2023-01-01T05:30:20Z', maxDate = "2023-01-01T05:30:20Z", keywords=['temperature', 'date'], eurobisDataset=['https://vocab.nerc.ac.uk/collection/P02/current/BPRP/'], eurobisColumn=['https://vocab.nerc.ac.uk/collection/P01/current/SNANID01/', 'https://vocab.nerc.ac.uk/collection/P01/current/SCNAME01/'], inTitle=['LOcal', 'study'], inLicense=['https://spdx.org/licenses/0BSD.html', 'redistributed'], inDescription=['Hazards', 'Sea Surface'], minDatasetCreationDate='2020-04-22T22:30:00Z', maxDatasetCreationDate='2020-04-22T22:55:00Z', format=['json', 'csv'], involvedList=['creator', 'publisher'], involvedName= ['Pete'], involvedMail= ['pete'], involvedUrl = ['https://www.chc.ucsb.edu/data/chirps'])
    # params: dict = dict(latitude=[], longitude=[])

    pd.set_option('display.max_colwidth', None)

    print('--------------- PARAMS ------------------------')
    params: dict = dict(latitude=[], longitude=[31,-38], temp='', format=['json'])
    print('params : {0}'.format(params))

    print('--------------- START UDAL --------------------')
    dal: UDAL = UDAL()

    print('--------------- Search for DATASETS ------------')
    datasetURL = dal.findMatchingDataset(**params)
    print('matching dataset : {0}'.format(datasetURL))

    print('--------------- get Basics PATTERNS ------------')
    basicUrl = dal.getBasicDatasetUrl()
    print('Basic Subset URL : {0}'.format(basicUrl))

    print('--------------- get Optimal PATTERN ------------')
    optiUrl = dal.getOptimalDatasetUrl()
    print('******************\nOptimal Subset URL : {0}'.format(optiUrl))

    print('--------------- Download Datasets -------------')
    downloadDatasets = dal.getDownloadDatasets()
    print('Download datasets : {0}'.format(downloadDatasets))

    print('--------------- Download Merged Datasets ------')
    downloadMergeDataset = dal.getDownloadMergedDatasets()
    print('Download Merged Dataset : {0}'.format(downloadMergeDataset))

    print('--------------- END ---------------------------')

    # provider: UDALProvider = FE_UDALProviderImpl()

    # # connect
    # auth_kwargs: dict= dict(...) # optional authentication settings
    # connection: UDALConnection = UDALConnection("http://example.org/udal-service-provider", **auth_kwargs)
    # broker: UDALBroker = provider.connect(connection)
    
    # # setup the question
    # query: UDALQuery = broker.namedQuery("http://example.org/23498769")
    # params: dict= dict(param_name="param_value", ..., ...)
    # query.validateParams(params)  # assert fitting data types / formats
    
    # # execute and process the responseanswer: 
    # UDALResult = broker.execute(query, params)
    
    # answer.data: pd:DataFrame     # access the data in dataframe format
    # answer.describe               # access the semantics of rows and cols in the dataframe
    # answer.prov                   # access the sources involved in producing the response
