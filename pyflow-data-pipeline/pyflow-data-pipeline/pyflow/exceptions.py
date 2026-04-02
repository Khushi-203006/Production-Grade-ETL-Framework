'''
2. Implement custom exception hierarchy: 
- DataSourceError
- ValidationError
- TransformationError
- LoadError
'''

#Base exceptionfor the pyflow project
class PyFlowError(Exception):
    pass

#Error when there is a problem reading data source
class DataSourceError(PyFlowError):
    pass

#Error when data validation fails
class ValidationError(PyFlowError):
    pass

#Error when transformation step fails
class TransformatioError(PyFlowError):
    pass

#Error when loading data to database fails
class LoaderError(PyFlowError):
    pass

