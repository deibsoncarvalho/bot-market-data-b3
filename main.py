from b3 import (
    update_historical_quotes_yearly,
    update_ibov,
    update_consolidated_data
)


if __name__ == "__main__":
    verbose = True
    update_ibov(verbose=verbose)
    update_consolidated_data(verbose=verbose)
    update_historical_quotes_yearly(verbose=verbose, from_year=2010)
