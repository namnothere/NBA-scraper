import pandas as pd
import requests

from bs4 import BeautifulSoup as bs

# CATALOG = pd.read_csv('pg_catalog.csv', na_filter=False)

#doneImage contain all the IDs and its URLs
#1,https://www.gutenberg.org/cache/epub/1/1-cover.png
#convert to dataframe
doneImage = pd.read_csv('images.txt', na_filter=False, names = ['ID', 'Image URL'])
#get all the IDs
# DONE_LIST = doneImage["ID"].tolist()

def getImageURL(ID: str):
    return 'https://www.example.com/images/' + ID + '.jpg'

def writeLog(message: str):
    f = open("[image]log_A.txt", "a")
    f.write(message + "\n")
    f.close()

def writeDone(url: str):
    f = open('[image]done_A.txt', 'a')
    f.write(url + '\n')
    f.close()

def getDone():
    try:
        f = open('[image]done_A.txt', 'r')
        done = f.read()
        f.close()
    except FileNotFoundError:
        done = ""
    return done

def csvHandle(csvPath):
    df = pd.read_csv(csvPath, na_filter=False)

    #Create a new column (Price) and set the value to 1.99
    # df['Price'] = 1.99

    #Get last row index that contain Image URL
    lastRow = df[df['Image URL'] != ""].index[-1]

    print("Last row with image URL:", lastRow)
    # return

    #Loop through every row and use the ID column to get the image URL
    for index, row in df.iterrows():

        #Skip the first 1000 rows
        if index < lastRow:
            continue

        try:
            
            if df.at[index, 'Image URL'] != "":
                continue
            df.at[index, 'Image URL'] = parseEpub(row['Text#'])
        except KeyboardInterrupt:
            print('KeyboardInterrupt')
            break
        except Exception as e:
            print('Error: Could not get image URL', e)


        if index % 1000 == 0:
            #Save the new CSV file
            df.to_csv('withimage.csv', index=False)
            print("Saved at", index)

    #Save the new CSV file
    df.to_csv('withimage.csv', index=False)
    print("Saved")

def getSoup(url: str) -> bs:
    try:
        # print("Getting url", url)
        response = requests.get(url)
        soup = bs(response.text, "html.parser")
        return soup
    except Exception as e:
        print("[getSoup]", e)
        return None

def parseEpub(ID: str):

    url = "https://www.gutenberg.org/cache/epub/{}/?C=S;O=D".format(ID)

    soup = getSoup(url)
    if soup == None:
        return False

    #Get the a tag with the .png or jpg at the end or any image extension and contains the word "cover"
    #Example: <a href="12376-cover.png">12376-cover.png</a>


    # img = soup.find("a", {"href": lambda L: L and L.endswith(".png") and "cover" in L})
    img = soup.find("a", {"href": lambda L: L and "cover" in L})

    if img == None:

        #Get the a tag with the .png or jpg at the end or any image extension and contains the word "cover"
        #Example: <a href="12376-cover.jpg">12376-cover.jpg</a>

        img = soup.find("a", {"href": lambda L: L and L.endswith(".jpg") and "cover" in L})

        if img == None:
            print("No image found")
            writeLog("No image found: " + url)
            return "0"
    
    #Get the href
    #Example: https://www.gutenberg.org/cache/epub/12376/12376-cover.png
    imgURL = (url.replace("/?C=S;O=D", "") + "/" + img["href"])
    return imgURL

def writeToFile(imgURL: str, ID: str):
    try:
        f = open("images.txt", "a")
        f.write(str(ID) + "," + imgURL + "\n")
        f.close()
    except Exception as e:
        print("[writeToFile]", e)
        return False

def loadImage():
    #read the images.txt file
    # f = open("images.txt", "r")
    # images = f.read()
    # f.close()
    global doneImage
    df = pd.read_csv('withImage.csv', na_filter=False)

    #get last row index that contain Image URL
    lastRow = df[df['Image URL'] != ""].index[-1]

    #write the images to the csv file in the Image URL column
    # for image in images.split("\n"):
    for index, row in df.iterrows():
        # if images.split() == "":
        #     print("No image found")
        #     continue

        #Copy the image url to the Image URL column

        if index < lastRow:
            continue

        try:
            ID = row['Text#']
            # imgURL = row['Image URL']

            # print("Writing image URL", imgURL, "to row", ID)

            # df.at[ID, 'Image URL'] = imgURL
            #write image url to df at Text# = ID
            #get index of ID
            # iDx = df[str(df['Text#']) == str(ID)].index.values.astype(int)[0]
            # CATALOG.loc[CATALOG['Text#'] == 2, 'Image URL'] = "test"
            # df.loc[index, 'Image URL'] = doneImage.loc[doneImage['ID'] == ID, 'Image URL'].values[0]

            # print("Updated row", doneImage.loc[doneImage['ID'] == ID, 'Image URL'].values[0])

            newURL = doneImage.loc[doneImage['ID'] == str(ID), 'Image URL'].values[0]
            df.at[index, 'Image URL'] = newURL

            print("Updated row", df.at[index, 'Image URL'])
            # return
            # break
            if index % 5000 == 0:
                print("Saved at", index)

        except KeyboardInterrupt:
            print('KeyboardInterrupt')
            df.to_csv('withimage_S.csv', index=False)
            break

        except Exception as e:
            print("Error: Could not write image URL", e)
            continue

        # print(ID, imgURL)

    #Save the new CSV file
    df.to_csv('test.csv', index=False)

def handleImage(ID: str):
    global DONE_LIST
    if str(ID) in DONE_LIST:
        # print("Already done")
        return
    # else:
        # print("Getting image URL for", ID)
        # print("Type:", type(ID))
    try:
        # print("Getting image URL for", ID)
        imgURL = parseEpub(ID)
        if imgURL == "0":
            # continue
            return
        print(imgURL)
        writeToFile(imgURL, ID)

    except KeyboardInterrupt:
        print('KeyboardInterrupt')
        # break
        return False
    except Exception as e:
        print('Error: Could not get image URL', e)


        # if index % 1000 == 0:
        #     print("Completed", index)

if __name__ == '__main__':

    url = "https://www.gutenberg.org/cache/epub/{}/?C=S;O=D"



    # csvHandle('withimage.csv')

    
            #Save the new CSV file
            # df.to_csv('withImage.csv', index=False)
            # print("Saved at", index)

    #Save the new CSV file
    # df.to_csv('withImage.csv', index=False)
    # print("Saved")

    # ========================================
    # allIDs = CATALOG["Text#"].apply(lambda x: x)
    
    # print(DONE_LIST)
    
    # print("Total IDs:", len(allIDs))
    # #use multiprocessing to speed up the process
    # from multiprocessing import Pool

    # pool = Pool(processes=8)
    # pass the url and the ID to the handleImage function
    # pool.map(handleImage, allIDs)

    # with Pool(processes=8) as pool:
        # issue tasks into the process pool
        # result = pool.map_async(handleImage, allIDs)
        # wait for tasks to complete
        # result.wait()
        # report all tasks done
        # print('All tasks are done', flush=True)

    # ========================================
    loadImage()

    #Load 
    #get the line where Text# == 2
    # line = CATALOG.loc[CATALOG['Text#'] == 2]
    # print(line)

    #update the line at columns Image URL
    # CATALOG.loc[CATALOG['Text#'] == 2, 'Image URL'] = "test"

    # print("Updated row", CATALOG.loc[CATALOG['Text#'] == 2, 'Image URL'].values[0])

    # df = pd.read_csv('images.txt', na_filter=False, names=['ID', 'Image URL'])

    #get the line where ID == 2
    # line = df.loc[df['ID'] == '2']
    # print(line)

    # print(df)