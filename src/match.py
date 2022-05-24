import os
import pickle
import requests
import time
from typing import Dict, List

import boto3
import pandas as pd
from sec_api import ExtractorApi, MappingApi, QueryApi


def add_to_s3(bucket, item_key: str, item_body):
    """Adds a writable object to s3.

    Args:
        bucket: The s3 bucket to add a file to.
        item_key: The name of the file being added.
        item_body: Information to write to the bucket.

    Returns:
        None
    """
    bucket.put_object(
        Key=item_key,
        Body=item_body
    )


def get_from_dynamo(table) -> List[Dict]:
    """Gets all items from a DynamoDB table.

    Args:
        table: The DynamoDB table name.

    Returns:
        All items in the DynamoDB.
    """
    response = table.scan()
    items = response["Items"]
    while "LastEvaluatedKey" in response:  # paginate due to 1MB return limit
        response = table.scan(ExclusiveStartKey=resposne["LastEvaluatedKey"])
        items.extend(response["Items"])

    return items


def get_data_from_url(url: str) -> Dict:
    """Uses requests lib to get data from a URL.

    Args:
        url: The site to get data from.

    Returns:
        The response information of requests lib.

    Raises:
        HTTPError: if an error occurred when attempting to access the url.
    """
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def get_ticker_data_fmp(ticker: str) -> List[Dict]:
    """Gets data of a ticker from the FMP API.

    Args:
        ticker: The ticker symbol of a company.

    Returns:
        All relevant tickers with information about each one.
    """
    return get_data_from_url(
        "https://financialmodelingprep.com/api/v3/profile/"+ ticker + "?apikey=" + 
            os.environ["FMP_API_KEY"]
    )


def get_sic_company_list_sec(sic_code: str) -> List[Dict]:
    """Gets companies for a given SIC code from the SIC API.

    Args:
        sic_code: The SIC (standard industry classifier) code for a company.

    Returns:
        All companies in the given SIC.
    """
    return get_data_from_url(
        "https://api.sec-api.io/mapping/sic/" + sic_code + "?token=" + os.environ["SEC_API_KEY"]
    )


def filter_by_market_cap(company_market_cap: float, tickers: List[str]) -> List[str]:
    """Filters similar companies for a SIC code given the target market cap.

    Args:
        company_market_cap: The desired market cap to match.
        tickers: Ticker symbols of similar companies.

    Returns:
        List of companies that are closest matching in market cap to the given market cap.
    """
    companies = []
    for ticker in tickers:
        ticker_info = get_ticker_data_fmp(ticker)
        if len(ticker_info) != 0:
            ticker_info = ticker_info[0]
            if ticker_info["cik"] is None or len(ticker_info["cik"]) < 2:
                continue
            ticker_market_cap = int(ticker_info["mktCap"])
            diff = abs(company_market_cap - ticker_market_cap)
            companies.append((ticker_info, diff))
        time.sleep(.2)  # 300 calls a minute

    return [
        str(int(info[0]["cik"])) for info in sorted(companies, key=lambda x: x[1], reverse=True)  # removes leading 000s
    ]


def find_largest_companies(tickers) -> List[str]:
    """Finds the largest companies for a SIC code by market cap.

    Args:
        tickers: Ticker symbols of companies in the same SIC.

    Returns:
        List of companies sorted by market cap in descending order.
    """
    companies = []
    for ticker in tickers:
        ticker_info = get_ticker_data_fmp(ticker)
        if len(ticker_info) != 0:
            ticker_info = ticker_info[0]
            if ticker_info["cik"] is None or len(ticker_info["cik"]) < 2:
                continue
            ticker_market_cap = int(ticker_info["mktCap"])
            companies.append((ticker_info, ticker_market_cap))
        time.sleep(.2)  # 300 calls a minute

    return [
        str(int(info[0]["cik"])) for info in sorted(companies, key=lambda x: x[1], reverse=True)
    ]


def get_similar_companies(map_api, cik: str) -> List[str]:
    """Finds the most relevant companies for a company.

    Args:
        map_api: SEC API to get company info.
        cik: A unique (central index) key to each company.

    Returns:
        List of relevant companies.
    """
    cik_info = map_api.resolve("cik", cik)[0]
    company_ticker = cik_info["ticker"]
    company_industry = cik_info["industry"]
    company_sic = cik_info["sic"]

    if company_ticker is None or company_ticker == "":
        return

    if company_industry != "":  # if a company has an industry then get a list of similar companies
        similar_companies = map_api.resolve("industry", company_industry)
    else:  # use the sec api to pull a list of similar companies
        similar_companies = get_sic_company_list_sec(company_sic)

    if len(similar_companies) == 0:
        print(f"No similar companies were found for {company_ticker}.")
        return

    df = pd.DataFrame.from_dict(similar_companies)
    similar_companies_df = df[(df["sic"] == company_sic) & ~df["isDelisted"]]  # remove delisted companies
    if similar_companies_df.shape[0] < 3:  # if there aren't enough similar companies, loosen restrictions
        similar_companies_df = df[~df["isDelisted"]]

    similar_companies = similar_companies_df["ticker"]
    similar_companies = similar_companies[similar_companies != "N/A"]

    company_info = get_ticker_data_fmp(company_ticker)
    if len(company_info) > 0:  # if the company still exists
        market_cap = int(company_info[0]["mktCap"])
        return filter_by_market_cap(market_cap, similar_companies)

    return find_largest_companies(similar_companies)


def find_time_ranges(company_info: List[Dict]) -> Dict[str, Dict]:
    """Finds the time range companies commited fraud for.

    Args:
        tickers: Ticker symbols of companies in the same SIC.

    Returns:
        List of companies sorted by market cap in descending order.
    """
    time_ranges = {}
    for info in company_info:
        if info["year_start"] > info["year_end"] or not info["contains_21c"]:
            continue
        if info["year_start"] == info["year_end"] and info["month_end"] - info["month_start"] < 6:
            print("Skipping company due to fraudulent activity being < 6 months.")
            continue
        if info["cik"] not in time_ranges:
            time_ranges[info["cik"]] = {
                "start_year": info["year_start"],
                "end_year": info["year_end"],
            }
        else:
            if time_ranges[info["cik"]]["start_year"] > info["year_start"]:
                time_ranges[info["cik"]]["start_year"] = info["year_start"]
            if time_ranges[info["cik"]]["end_year"] > info["year_end"]:
                time_ranges[info["cik"]]["end_year"] = info["year_end"]

    return time_ranges


def get_company_info(query_api, cik: str, start_year: str, end_year: str) -> Dict:
    """Gets 10-Ks and other filings for a company from start to end year.

    Args:
        query_api: SEC API to get a company's filing records.
        cik: A unique (central index) key to each company.
        start_year: The year the company started committing fraud.
        end_year: the year the company stopped committing fraud.

    Returns:
        A dictionary containing info about the company's filings.
    """
    query = {
        "query": {
            "query_string": {
                "query": "cik: \"%s\" AND filedAt:{%s-01-01 TO %s-12-31}" % (cik, start_year, end_year) + \
                    " AND formType: \"10-K\" AND documentFormatFiles.type: \"10-K\""
            }
        }
    }

    filings = query_api.get_filings(query)
    return filings


def add_10k_info(extractor_api, bucket, urls: List[Dict[str, str]]) -> None:
    """Adds 10-K documents to the S3 bucket.

    Args:
        extractor_api: SEC API to get 10-K from their respective links.
        bucket: The S3 bucket to add the files to.
        urls: A list of dictionaries containing information about a 10-K document.
    """
    for url_object in urls:
        url = url_object["url"]
        item = {
            "url": url,
            "1A": extractor_api.get_section(url, "1A", "text"),
            "7": extractor_api.get_section(url, "7", "text"),
            "7A": extractor_api.get_section(url, "7A", "text"),
        }
        item_pickle = pickle.dumps(item)
        add_to_s3(bucket, "nonfraudulent/{}/{}.pkl".format(url_object["cik"], url_object["year"]), item_pickle)
        print("Add attempted.")


def add_nonfraud_urls(extractor_api, map_api, query_api, bucket, dynamo_table) -> None:
    """Main process to add non-fraudulent 10-Ks to S3.

    Args:
        extractor_api: SEC API to get 10-K from their respective links.
        map_api: SEC API to get company info.
        query_api: SEC API to get a company's filing records.
        bucket: The S3 bucket to add the files to.
        dynamo_table: The DynamoDB table name.
    """
    fraud_company_info = get_from_dynamo(dynamo_table)
    time_ranges = find_time_ranges(fraud_company_info)

    similar_companies = {}
    for cik in time_ranges:
        companies = get_similar_companies(map_api, cik)
        if companies is None:
            continue
        similar_companies[cik] = {
            "companies": [company for company in companies if company not in time_ranges],  # make sure company did not commit fraud
            "start_year": str(int(time_ranges[cik]["start_year"])),
            "end_year": str(int(time_ranges[cik]["end_year"])),
        }

    urls = []
    for key in similar_companies:
        matched = 0
        for cik in similar_companies[key]["companies"]:
            filings = get_company_info(
                query_api,
                cik,
                similar_companies[key]["start_year"],
                similar_companies[key]["end_year"],
            )
            if "filings" in filings and len(filings["filings"]) > 0:
                found = False
                for filing in filings["filings"]:
                    for document in filing["documentFormatFiles"]:
                        if document["type"].lower() == "10-k":
                            urls.append(
                                {
                                    "url": document["documentUrl"],
                                    "cik": key,
                                    "year": filing["filedAt"][:4],
                                }
                            )
                            found = True
                if found:
                    matched += 1

            if matched == 2:  # want to pair a fraud document with ~2 non-fraud documents
                break  # the lower down the similar company list the less similar the 10-Ks

    add_10k_info(extractor_api, bucket, urls)


if __name__ == "__main__":
    # initialize resources
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["DYNAMO_TABLE"])

    sec_extractor_api = ExtractorApi(api_key=os.environ["SEC_API_KEY"])
    sec_mapping_api = MappingApi(api_key=os.environ["SEC_API_KEY"])
    sec_query_api = QueryApi(api_key=os.environ["SEC_API_KEY"])

    # make sure to specify task permission for ECS to access s3
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(os.environ["S3_BUCKET"])

    add_nonfraud_urls(sec_extractor_api, sec_mapping_api, sec_query_api, bucket, table)
