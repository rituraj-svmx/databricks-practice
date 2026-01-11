from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

@udf(returnType=FloatType())
def inr_to_usd(inr):
    """
    Returns the exchange rate of INR to USD.and the rate is 91
    """
    return inr / 91

