import pandas as pd

INPUT_FILE = ''
OUTPUT_FILE = ''

def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = pd.read_csv(INPUT_FILE)
    
    #drop duplicates
    df.drop_duplicates(inplace=True)

    df.to_csv(OUTPUT_FILE, index=False)