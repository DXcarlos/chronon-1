from typing import Optional

import ai.chronon.api.ttypes as ttypes


class ModelType:
    XGBoost = ttypes.ModelType.XGBoost
    PyTorch = ttypes.ModelType.PyTorch


# Name must match S3 path that we expose if you're uploading trained models?
def Model(
    source: ttypes.Source,
    outputSchema: ttypes.TDataType,
    modelType: ModelType,
    name: str = None,
    modelParams: Optional[dict[str, str]] = None
) -> ttypes.Model:
    if not isinstance(source, ttypes.Source):
        raise ValueError("Invalid source type")
    if not (isinstance(outputSchema, ttypes.TDataType) or isinstance(outputSchema, int)):
        raise ValueError("outputSchema must be a TDataType or DataKind")
    if isinstance(outputSchema, int):
        # Convert DataKind to TDataType
        outputSchema = ttypes.TDataType(outputSchema)

    if modelParams is None:
        modelParams = {}

    metaData = ttypes.MetaData(
        name=name,
    )

    return ttypes.Model(modelType=modelType, outputSchema=outputSchema, source=source,
                        modelParams=modelParams, metaData=metaData)
