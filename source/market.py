import korea as krx
import us
import fred as fd
import yaml, os, types, datetime
import numpy as np
import pandas as pd

PERIODS = 365*10

def get_yaml(yaml_name:str):
    yaml_file_path = os.path.join(os.path.dirname(__file__),'..','yaml')
    #if not os.path.exists(yaml_file_path):
    #    os.mkdir(yaml_file_path)
    try:
        with open(os.path.join(yaml_file_path, '%s.yaml'%yaml_name)) as f:
            conf = yaml.load(f, Loader=yaml.FullLoader)
    except:
        print('%s.yaml does not exist'%yaml_file_path)
        conf = {}
    return conf

def get_etf_from_yaml(yaml_name):
    """ Return
      dataframe
    """
    yaml = get_yaml(yaml_name)

    etf_name = np.array([])
    etf_code = np.array([])
    cmds = np.array([], np.int32)
    ratios = np.array([], np.float32)
    sources = np.array([])

    sub_portpolio = []
    sub_ratios = []

    for i,label in enumerate(yaml[yaml_name].values()):
        for sub_label in label:
            if sub_label['etf'].find('/') != -1:
                sp, sr = get_etf_from_yaml(sub_label['etf'][1:])
                sr = [(sub_label['ratio']/100)*s for s in sr]
                sub_portpolio.append(sp)
                sub_ratios.append(sr)
                continue

            etf_code   = np.append(etf_code, sub_label['etf'])  
            etf_name   = np.append(etf_name, sub_label['comment'])
            ratios = np.append(ratios,sub_label['ratio'])
            sources = np.append(sources,sub_label['source'])
            cmds = np.append(cmds, i)

    cols=['Type', 'Name', 'Ticker', 'Src', 'Ratio']
    items = pd.DataFrame([],columns=cols)

    for i,etf in enumerate(etf_code):
        tmp = list(yaml[yaml_name].keys())
        df_temp = pd.DataFrame(
                [[tmp[cmds[i]], etf_name[i], etf_code[i], sources[i], ratios[i]]],
                columns=cols
            )
        items = pd.concat([items,df_temp],ignore_index=True)
    return items

def get_df_from_yaml(yaml_name:str):
    portfolio = get_etf_from_yaml('AW4_11')
    
    dt = pd.date_range(end=datetime.datetime.today().strftime('%Y-%m-%d'), periods=PERIODS, freq='D')
    pf_merge_df = pd.DataFrame(dt,columns=['Date']).iloc[::-1].set_index('Date')

    # USD/KRW
    inst = us.US()
    pf_df = inst.get_instance('KRW=X')
    if pf_df.empty:
        pf_df = inst.create_instance(['KRW=X'])
        pf_df = inst.get_instance('KRW=X')

    pf_df = pf_df.iloc[::-1]
    pf_merge_df['USD/KRW'] = pf_df[['Close']].apply(lambda x: round(x,2))
    
    # Portfolio
    for i in range(len(portfolio)):
        pf_src, pf_ticker, pf_name = portfolio.iloc[i].Src, portfolio.iloc[i].Ticker, portfolio.iloc[i].Name
        if pf_src in krx.source_list:
            inst = krx.Korea()
            pf_df = inst.get_instance(pf_ticker)
            if pf_df.empty:
                pf_df = inst.create_instance([pf_ticker])
                pf_df = inst.get_instance(pf_ticker)
        elif pf_src in us.source_list:
            inst = us.US()
            pf_df = inst.get_instance(pf_ticker)
            if pf_df.empty:
                pf_df = inst.create_instance([pf_ticker])
                pf_df = inst.get_instance(pf_ticker)
        elif pf_src in fd.source_list:
            inst = fd.FredIndex()
            pf_df = inst.get_instance(pf_ticker)
            if pf_df.empty:
                pf_df = inst.create_instance([pf_ticker])
                pf_df = inst.get_instance(pf_ticker)
        else:
            pf_df = pd.DataFrame([])

        pf_df = pf_df.iloc[::-1]
        pf_merge_df[pf_name] = pf_df[['Close']]

        if pf_src in us.source_list:
            pf_merge_df[pf_name] = pf_merge_df[pf_name] * pf_merge_df['USD/KRW']

        pf_merge_df[pf_name] = pf_merge_df[pf_name].apply(lambda x: round(x,2))

    return pf_merge_df.dropna()

if __name__ == '__main__':
    ret = get_df_from_yaml('AW4_11')
    print(len(ret))
    print(ret.head(10))

    print(ret.tail(10))
