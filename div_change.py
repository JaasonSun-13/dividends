import blpapi
from xbbg import blp
from blpapi import SessionOptions, Session, Service

def fetch_sp500_tickers():
    # Bloomberg API service and request details
    service_uri = "//blp/mktdata"
    ref_service_uri = "//blp/refdata"

    session_options = SessionOptions()
    session_options.setServerHost('localhost')
    session_options.setServerPort(8194)

    session = Session(session_options)

    if not session.start():
        print("Failed to start session.")
        return
    if not session.openService(ref_service_uri):
        print("Failed to open refdata service.")
        return

    ref_data_service = session.getService(ref_service_uri)
    request = ref_data_service.createRequest("ReferenceDataRequest")

    # Use S&P 500 index membership to pull tickers
    # Request members of S&P 500 Index
    request.getElement("securities").appendValue("SPX Index")

    # Fields to retrieve: ticker, name, etc.
    request.getElement("fields").appendValue("INDX_MEMBERS")

    print("Sending request...")
    session.sendRequest(request)

    # Process response
    tickers = []
    while True:
        event = session.nextEvent()
        for msg in event:
            if msg.hasElement("securityData"):
                security_data = msg.getElement("securityData")
                # For SPX, get the INDX_MEMBERS field
                field_data = security_data.getValue(0).getElement("fieldData")
                if field_data.hasElement("INDX_MEMBERS"):
                    members = field_data.getElement("INDX_MEMBERS")
                    for i in range(members.numValues()):
                        member = members.getValue(i)
                        ticker = member.getElementAsString("Member Ticker and Exchange Code")
                        tickers.append(ticker)
        if event.eventType() == blpapi.Event.RESPONSE:
            break

    session.stop()

    return tickers


sp500_tickers = fetch_sp500_tickers()
print(f"Found {len(sp500_tickers)} tickers:")
print(sp500_tickers)

for tick in sp500_tickers:
    df = blp.bdh(
        tickers=tick+' Equity',
        flds=['EQY_DVD_YLD_IND'],
        start_date='2014-01-01',
        end_date='2024-01-01',
        Per='D'
)
    start_yield=df["EQY_DVD_YLD_IND"][0]
    for tick_yield in df['EQY_DVD_YLD_IND']:
        df['YILD_PCT_CHANGE']=tick_yield/start_yield*100
    for yield_pct_change in df['YILD_PCT_CHANGE']:
    
        if abs(tick_yield-start_yield)<0.1:
            df=df[df['EQY_DVD_YLD_IND'] if ]
    print(df.tail())
