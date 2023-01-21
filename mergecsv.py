import pandas as pd

# Read the csv files
# df1 = pd.read_csv('BoxScore_Part1c.csv')
# df2 = pd.read_csv('BoxScore_Part2c.csv')
# df3 = pd.read_csv('BoxScore_Part3c.csv')
# df = pd.read_csv('cleanedBoxScore.csv')
df = pd.read_csv('BoxScore_regular_2023-01-20.csv')

# Concat the dataframes
# df = pd.concat([df1, df2, df3], ignore_index=True)

# Duplicate columns will be saved in another csv file
# df[df.duplicated()].to_csv('duplicate.csv', index=False)

# Count the number of duplicate columns
# print(df.duplicated().sum())

# Drop the duplicate columns
df = df.drop_duplicates()


inactive = ['Did Not Dress', 'Did Not Play', 'Player Suspended', 'Not With Team']
# Drop rows where it contains 'Did Not Play' in any column
df = df[~df.isin(inactive).any(axis=1)]

# Drop rows where it does not contains any number in columns from index 1 to 19
df = df[df.iloc[:, 1:19].apply(lambda x: x.str.contains('[0-9]').any(), axis=1)]

# Sort the dataframe by the column 'Date'
df = df.sort_values(by='Date')

# Swap the order of the columns
# Put URL column at the end
cols = df.columns.tolist()
urlCol = cols.pop(cols.index('URL')) # Remove URL column and save it
cols.append(urlCol) # Append URL column to the end
df = df[cols]


#=====================================================
# Drop row that date is after Oct 18, 2022 and before 2015-04-01
# df = df[df['Date'] < '2022-10-18'] 
# df = df[df['Date'] > '2015-04-01']

# Write the dataframe to a new csv file
df.to_csv('BoxScore_Without_InactivePlayer.csv', index=False)

# Path: mergecsv.py
