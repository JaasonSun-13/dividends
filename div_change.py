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

count = 0
for tick in sp500_tickers:
    if count == 20:
        break
    count+=1
    df = blp.bdh(
        tickers=tick+' Equity',
        flds=['DIVIDEND_INDICATED_YIELD','EQY_INST_BUYS','EQY_INST_SELLS'],
        start_date='2014-01-01',
        end_date='2024-01-01',
        Per='D'
)
    col = (tick+' Equity', "DIVIDEND_INDICATED_YIELD")
    buyers = (tick+' Equity', "EQY_INST_BUYS")
    sellers = (tick+' Equity', "EQY_INST_SELLS")
    fld_list=[col , buyers , sellers]

    if not all(col in df.columns for col in  fld_list):
        print( f'tick {tick} missing data')
        continue
    
    if not df.empty:

        df['YILD_PCT_CHANGE'] = df[col].pct_change() * 100
        df['NET_BUYERS']=(df[buyers]-df[sellers])
        df['NET_BUYERS_10D_AVG'] = df['NET_BUYERS'].rolling(window=10).mean()
        df['NET_BUYERS_VS_10D'] = df['NET_BUYERS'] / df['NET_BUYERS_10D_AVG']  
        
        df_filtered = df[df['YILD_PCT_CHANGE'].abs() > 10]
        print(df)
        df.to_excel(f'{tick}.xlsx')
        if not df_filtered.empty:
            print(df[["YILD_PCT_CHANGE", 'NET_BUYERS_VS_10D']])
        # print(df.columns)

