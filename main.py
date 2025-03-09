import datetime
import json
import random
import time
import uuid
import mysql.connector
import requests
import schedule


#=============================================



#============================================

def find_option_combinations(data, key):
    # key를 찾았을 경우 반환
    if isinstance(data, dict):
        if key in data:
            return data[key]
        else:
            # dictionary 내부를 재귀적으로 탐색
            for value in data.values():
                result = find_option_combinations(value, key)
                if result is not None:
                    return result
    elif isinstance(data, list):
        # list 내부의 각 아이템을 재귀적으로 탐색
        for item in data:
            result = find_option_combinations(item, key)
            if result is not None:
                return result
def get_user_history(connection):
    try:
        # MySQL 연결 설정
        # connection = mysql.connector.connect(
        #     host='43.200.242.185',
        #     port=3306,
        #     user='toys12',
        #     password='!qaz2wsx',
        #     database='insta'
         
        # )
        
        # 커서 생성
        cursor = connection.cursor()

        # # USER 테이블에 데이터 삽입
        # insert_query = """
        # INSERT INTO USER (PLATFORM,USERNAME, PAGE_URL, REG_DTM, MDFY_DTM) 
        # VALUES (%s, %s, %s, NOW(), NOW())
        # """
        # values = ('I', 'xxxibgdrgn', 'https://www.instagram.com/xxxibgdrgn/')
        
        # cursor.execute(insert_query, values)
        # connection.commit()
        
        # USER 테이블의 스키마 조회


        cursor.execute("DESCRIBE USER")
        
        # 결과 가져오기
        schema = cursor.fetchall()
        
        # 스키마 정보 출력
        print("\n[테이블 스키마 정보]")
        for column in schema:
            print(f"컬럼명: {column[0]}, 데이터타입: {column[1]}, Null허용: {column[2]}, Key: {column[3]}, Default: {column[4]}")

        # USER 테이블 조회
        cursor.execute("SELECT * FROM USER")

        
        # 결과 가져오기
        results = cursor.fetchall()

        
        
        # 결과 출력
        for row in results:
            print(row)

        # 결과를 딕셔너리 형태로 변환
        dict_results = []
        for row in results:
            dict_row = {
                'ID': row[0],
                'PLATFORM': row[1], 
                'USERNAME': row[2],
                'PAGE_URL': row[3],
                'REG_DTM': row[4].strftime('%Y-%m-%d') if row[4] else None,  # 날짜를 문자열로 변환
                'MDFY_DTM': row[5].strftime('%Y-%m-%d') if row[5] else None  # 날짜를 문자열로 변환
            }
            dict_results.append(dict_row)
        
        results = dict_results
            
    except mysql.connector.Error as error:
        print(f"MySQL 연결 오류: {error}")
        

    return results

def GetDetailProfile(inputData):
    username=inputData['USERNAME']

    url = "https://real-time-instagram-scraper-api1.p.rapidapi.com/v1/user_info"

    querystring = {"username_or_id":username}

    headers = {
        "x-rapidapi-key": "2e4a105e81mshe8534d478890bf7p14afbdjsnf894b6f56b1f",
        "x-rapidapi-host": "real-time-instagram-scraper-api1.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    
    results=json.loads(response.text)
    followerCount=find_option_combinations(results,'follower_count')
    return followerCount

def GetInstaProfile(inputData):
    print("instagram 프로필 가져오기")
    url_parts = inputData['PAGE_URL'].split('/')
    username = url_parts[-1] if url_parts[-1] else url_parts[-2]
    print(username)
    
    

def GetInsta(inputData):
    print("instagram")
    # url_parts = inputData['PAGE_URL'].split('/')
    # shortcode = url_parts[-1] if url_parts[-1] else url_parts[-2]
    # print(shortcode)
    page_url=inputData['PAGE_URL']

    url = "https://real-time-instagram-scraper-api1.p.rapidapi.com/v1/media_info"
    querystring = {"code_or_id_or_url":page_url}

    headers = {
        "x-rapidapi-key": "2e4a105e81mshe8534d478890bf7p14afbdjsnf894b6f56b1f",
        "x-rapidapi-host": "real-time-instagram-scraper-api1.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    results=json.loads(response.text)
    with open('results.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    shareCount=0
    likeCount=0
    commentCount=0
    viewCount=0
    reshareCount=0
    try:
        likeCount=find_option_combinations(results,'like_count')
        commentCount=find_option_combinations(results,'comment_count')
        viewCount=find_option_combinations(results,'play_count')
        reshareCount=find_option_combinations(results,'reshare_count')
        
        print("불러오기 완료")
    except:
        print("json 파일 로드 실패")
        return
    return likeCount, commentCount, viewCount, shareCount,reshareCount
    

    
    

def GetFacbook(inputData):
    searchParams=''
    searchCase=""
    print("PAGE_URL:",inputData['PAGE_URL'])
    
    if inputData['PAGE_URL'].find('?fbid=')>=0:
        searchParams=inputData['PAGE_URL']
        searchCase="A"
    elif inputData['PAGE_URL'].find('?id=')>=0:
        searchParams = inputData['PAGE_URL'].split('?id=')[-1]
        searchCase="B"
    else:
        searchParams=inputData['PAGE_URL']
        searchCase="C"
    print('searchParams:',searchParams)
    print('searchCase:',searchCase)
    followerCount=0
    likeCount=0
    reshareCount=0
    viewCount=0
    commentCount=0
    if searchCase=="A":
        url = "https://facebook-scraper3.p.rapidapi.com/post"

        querystring = {"post_url":searchParams}

        headers = {
            "x-rapidapi-key": "2e4a105e81mshe8534d478890bf7p14afbdjsnf894b6f56b1f",
            "x-rapidapi-host": "facebook-scraper3.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers, params=querystring)
        results=json.loads(response.text)
        try:
            likeCount=find_option_combinations(results,'reactions_count')
        except:
            likeCount=0
        try:
            reshareCount=find_option_combinations(results,'reshare_count')
        except:
            reshareCount=0
        try:
            commentCount=find_option_combinations(results,'comments_count')
        except:
            commentCount=0

    elif searchCase=="B":
        url = "https://facebook-realtimeapi.p.rapidapi.com/facebook/profiles/{}/followers".format(searchParams)

        headers = {
            "x-rapidapi-key": "2e4a105e81mshe8534d478890bf7p14afbdjsnf894b6f56b1f",
            "x-rapidapi-host": "facebook-realtimeapi.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers)
        results=json.loads(response.text)
        try:
            followerCount=find_option_combinations(results,'full_profile_list').get('list_items',{}).get('count',0)
        except:
            followerCount=0


    elif searchCase=="C":
        url = "https://facebook-scraper3.p.rapidapi.com/page/details"

        querystring = {"url":searchParams}

        headers = {
            "x-rapidapi-key": "2e4a105e81mshe8534d478890bf7p14afbdjsnf894b6f56b1f",
            "x-rapidapi-host": "facebook-scraper3.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers, params=querystring)
        results=json.loads(response.text)
        try:
            followerCount=find_option_combinations(results,'followers')
        except:
            followerCount=0


        
    results=json.loads(response.text)
    with open('results_{}.json'.format(searchCase), 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    print("followerCount:",followerCount,"likeCount:",likeCount,"commentCount:",commentCount,"viewCount:",viewCount,"reshareCount:",reshareCount)
    return followerCount,likeCount,commentCount,viewCount,reshareCount
def run():
#=========프로필 정보 가져오기
# DB 정보를 입력해주세요
    connection=mysql.connector.connect(
            host='43.200.242.185',
            port=3306,
            user='toys12',
            password='!qaz2wsx',
            database='insta')
    try:
        cursor = connection.cursor()
        print("데이터베이스 연결 성공")
    except Exception as e:
        print("데이터베이스 연결 실패:", str(e))
        return
    results = get_user_history(connection)
    with open('results.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4)


    with open('results.json', 'r', encoding='utf-8') as f:
        results = json.load(f)
    # 파일 경로 지정
    for result in results:
        # if result['PLATFORM'] == 'F':
        #     print("===============")
        #     continue
        
        platform=result['PLATFORM']
        username=result['USERNAME']
        url=result['PAGE_URL']
        print(f"platform:{platform}, username:{username}, url:{url}")


        if platform == 'I':
            likeCount, commentCount, viewCount, shareCount,reshareCount=GetInsta(result)
            time.sleep(random.randint(2, 3))
            followerCount=GetDetailProfile(result)
            timeNow=datetime.datetime.now().strftime("%Y-%m-%d")
            followerData=["A",followerCount,timeNow,result['ID']]
            likeData=["B",likeCount,timeNow,result['ID']]
            viewData=["C",viewCount,timeNow,result['ID']]
            commentData=["D",commentCount,timeNow,result['ID']]
            reshareData=["E",reshareCount,timeNow,result['ID']]

            try:                
                cursor = connection.cursor()

                # USER_HIS 테이블에 데이터 삽입
                insert_query = """
                INSERT INTO USER_HIS (STATE,COUNT,REG_DTM,USERID)
                VALUES (%s, %s, %s,%s)
                """
                print('followerData:',followerData)
                print('likeData:',likeData)
                print('viewData:',viewData)
                print('commentData:',commentData)
                print('reshareData:',reshareData)

                # 각 데이터 삽입
                if followerCount != 0 and followerCount != None:
                    cursor.execute(insert_query, followerData)
                if likeCount != 0 and likeCount != None:
                    cursor.execute(insert_query, likeData) 
                if viewCount != 0 and viewCount != None:
                    cursor.execute(insert_query, viewData)
                if commentCount != 0 and commentCount != None:
                    cursor.execute(insert_query, commentData)
                if reshareCount != 0 and reshareCount != None:
                    cursor.execute(insert_query, reshareData)

                connection.commit()
                print("데이터가 성공적으로 삽입되었습니다.")

            except mysql.connector.Error as error:
                print(f"MySQL 연결 오류: {error}")
                
            # finally:
            #     if 'connection' in locals() and connection.is_connected():
            #         cursor.close()
            #         connection.close()
            #         print("MySQL 연결이 종료되었습니다.")
            
            print(f"followerCount:{followerCount}, likeCount:{likeCount}, commentCount:{commentCount}, viewCount:{viewCount}")
        elif platform == 'F':
            followerCount,likeCount,commentCount,viewCount,reshareCount=GetFacbook(result)
            time.sleep(random.randint(2, 3))
            try:
                # MySQL 연결 설정
                
                cursor = connection.cursor()

                # USER_HIS 테이블에 데이터 삽입
                insert_query = """
                INSERT INTO USER_HIS (STATE,COUNT,REG_DTM,USERID)
                VALUES (%s, %s, %s,%s)
                """

                timeNow=datetime.datetime.now().strftime("%Y-%m-%d")
                followerData=["A",followerCount,timeNow,result['ID']]
                likeData=["B",likeCount,timeNow,result['ID']]
                viewData=["C",viewCount,timeNow,result['ID']]
                commentData=["D",commentCount,timeNow,result['ID']]
                reshareData=["E",reshareCount,timeNow,result['ID']]

                print('followerData:',followerData)
                print('likeData:',likeData)
                print('viewData:',viewData)
                print('commentData:',commentData)
                print('reshareData:',reshareData)

                # 각 데이터 삽입
                if followerCount != 0 and followerCount != None:
                    cursor.execute(insert_query, followerData)
                if likeCount != 0 and likeCount != None:
                    cursor.execute(insert_query, likeData) 
                if viewCount != 0 and viewCount != None:
                    cursor.execute(insert_query, viewData)
                if commentCount != 0 and commentCount != None:
                    cursor.execute(insert_query, commentData)
                if reshareCount != 0 and reshareCount != None:
                    cursor.execute(insert_query, reshareData)

                connection.commit()
                print("데이터가 성공적으로 삽입되었습니다.")

            except mysql.connector.Error as error:
                print(f"MySQL 연결 오류: {error}")
                

            
        print("=================================")
        time.sleep(random.randint(2, 3))

    connection.close()
    print("DB 연결이 종료되었습니다.")

print("실행")
# 처음 한 번 실행



run()

# 매일 오전 6시에 실행되도록 스케줄 설정
schedule.every().day.at("06:00").do(run)

# 스케줄 유지를 위한 무한 루프
timeNow=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
while True:
    print("현재시간은:",timeNow)
    schedule.run_pending()
    time.sleep(60)
