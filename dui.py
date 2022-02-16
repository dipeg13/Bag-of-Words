import numpy as np
import pandas as pd
import pymongo as pmg
import folium
from tslearn.metrics import lcss
import matplotlib.pyplot as plt
from time import time
from datetime import datetime

print('Connecting to database...')
client = pmg.MongoClient(host="localhost", port=27017)

def reporting_points(file_name, cursor):
    print('Rendering just started...')
    coor = []
    for i in cursor:
        coor.append(i['location']['coordinates'])
                
    Map = folium.Map()
    folium.CircleMarker(radius=3, fill=False, location=[lat, lon], color='red').add_to(Map)

    for i in range(len(coor)):
        folium.CircleMarker(radius=1, fill=True, location=[coor[i][1],coor[i][0]], color='blue').add_to(Map)

    Map.save(file_name+'.html')
    print('Render just ended!')
    
try:
    client.admin.command('ismaster')
    print("Connected Sucessfully")


    db = client['task_mongo']
    traj = db['coorData']
    raw = db['nari_raw']

    def menu():
        global raw
        query_type = int(input('1 : Relational Queries\n2 : Spatial Queries\n3 : Spatio-temporal Queries\n4 : Trajectory Queries\n'))
        if query_type == 1:
            relational()
        elif query_type == 2:
            spatial() #DONE
        elif query_type == 3:
            spatiotemporal() #DONE
        elif query_type == 4:
            trajectory() #DONE
        else:
            print('Invalid input, please try again')
            menu()

    #OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
            
    def relational():
        pass

    #OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
    
    def spatial():
        query_type = int(input('1 : k-nearest neighbors\n2 : Circle Range Query\n3 : Torus Range Query\n4 : Polygon Range Query\n'))
        if query_type == 1:
            knn_spatial() #DONE
        elif query_type == 2:
            circle_spatial() #DONE
        elif query_type == 3:
            torus_spatial() #DONE
        elif query_type == 4:
            triangle_spatial() #DONE
        else:
            flag = True
            while flag:
                kappa = int(input('Invalid input, give 0 to main menu or 1 to spatial menu\n'))
                if kappa == 0 or kappa == 1:
                    flag = False
            if kappa == 0:
                menu()
            else:
                spatial()
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    def knn_spatial():
        try:
            global raw
            k = int(input('Give k.\n'))
            lon = float(input('Give longtitude.\n'))
            lat = float(input('Give latitude.\n'))
            tic = time()
            cursor = raw.aggregate([{'$geoNear':{'near':{'type':'Point', 'coordinates':[lon, lat]},
                                       'distanceField': 'dist.calculated', 'includeLocs': 'dist.location',
                                       'spherical': 'true'}}, {'$limit': k}])
            print('Aggregation ended in', str(round(time()-tic, 5)), 'seconds')
            #----------------------------------------------------------------------------------------------------------
            file_name = input('Give file name.\n')
            reporting_points(file_name, cursor)
            #----------------------------------------------------------------------------------------------------------
            flag = True
            while flag:
                gate = int(input('0 : k-nearest neighbors\n1 : main menu\n'))
                if gate ==0 or gate == 1:
                    flag = False
            if gate == 0:
                knn_spatial()
            else:
                menu()
        except:
            print('Error/s occured. Please try again.')
            menu() 

    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    def circle_spatial():
        try:
            global raw
            radius_miles = int(input('Give radius in miles.\n'))
            lon = float(input('Give longtitude.\n'))
            lat = float(input('Give latitude.\n'))
            tic = time()
            cursor = raw.aggregate([{'location':{'$geoWithin':{'$centerSphere':[[lon, lat], radius_miles/3963.2]}}}])
            
            print('Aggregation ended in', str(round(time()-tic, 5)), 'seconds')
            #----------------------------------------------------------------------------------------------------------
            file_name = input('Give file name.\n')
            reporting_points(file_name, cursor)   
            #----------------------------------------------------------------------------------------------------------
            flag = True
            while flag:
                gate = int(input('0 : k-nearest neighbors\n1 : main menu\n'))
                if gate ==0 or gate == 1:
                    flag = False
            if gate == 0:
                knn_spatial()
            else:
                menu()
        except:
            print('Error/s occured. Please try again.')
            menu()
    
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    def torus_spatial():
        try:
            global raw
            dismin = int(input('Give minimum distance.\n'))
            dismax = int(input('Give maximum distance.\n'))
            lon = float(input('Give longtitude.\n'))
            lat = float(input('Give latitude.\n'))
            tic = time()
            cursor = raw.aggregate([{'$geoNear':{'near':{'type':'Point', 'coordinates':[lon, lat]},
                                       'distanceField':'dist.calculated', 'maxDistance':dismax, 'minDistance':dismin,'spherical':'ture'}}])
            print('Aggregation ended in', str(round(time()-tic, 5)), 'seconds')
            #----------------------------------------------------------------------------------------------------------
            file_name = input('Give file name.\n')
            reporting_points(file_name, cursor)
            #----------------------------------------------------------------------------------------------------------
            flag = True
            while flag:
                gate = int(input('0 : k-nearest neighbors\n1 : main menu\n'))
                if gate ==0 or gate == 1:
                    flag = False
            if gate == 0:
                knn_spatial()
            else:
                menu()
        except:
            print('Error/s occured. Please try again.')
            menu() 
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    def triangle_spatial():
        try:
            global raw
            lon1 = float(input('Give first longtitude.\n'))
            lat1 = float(input('Give first latitude.\n'))
            
            lon2 = float(input('Give second longtitude.\n'))
            lat2 = float(input('Give second latitude.\n'))
            
            lon3 = float(input('Give third longtitude.\n'))
            lat3 = float(input('Give third latitude.\n'))
            
            tic = time()
            cursor = raw.aggregate([{'$match':{'location':{'$geoWithin':{'$geometry':{'type': 'Polygon' ,
                                                                            'coordinates': [ [ [lon1,lat1],[lon2,lat2],[lon3,lat3]]]}}}}}])
            print('Aggregation ended in', str(round(time()-tic, 5)), 'seconds')
            #----------------------------------------------------------------------------------------------------------
            file_name = input('Give file name.\n')
            reporting_points(file_name, cursor)
            #----------------------------------------------------------------------------------------------------------
            flag = True
            while flag:
                gate = int(input('0 : k-nearest neighbors\n1 : main menu\n'))
                if gate ==0 or gate == 1:
                    flag = False
            if gate == 0:
                knn_spatial()
            else:
                menu()
        except:
            print('Error/s occured. Please try again.')
            menu() 
            
    #OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
            
    def spatiotemporal():
        query_type = int(input('1 : k-nearest neighbors\n2 : Circle Range Query\n3 : Torus Range Query\n4 : Polygon Range Query\n'))
        if query_type == 1:
            knn_spatiotemp() #DONE
        elif query_type == 2:
            circle_spatiotemp() #DONE
        elif quesry_type == 3:
            torus_spatiotemp() #DONE
        elif query_type == 4:
            triangle_spatiotemp() #DONE
        else:
            flag = True
            while flag:
                kappa = int(input('Invalid input, give 0 to main menu or 1 to spatiotemporal menu\n'))
                if kappa == 0 or kappa == 1:
                    flag = False
            if kappa == 0:
                menu()
            else:
                spatiotemporal()
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    def knn_spatiotemp():
        try:
            global raw
            tmin = int(input('Give minimum time.\n'))
            tmax = int(input('Give maximum time.\n'))
            k = int(input('Give k.\n'))
            lon = float(input('Give longtitude.\n'))
            lat = float(input('Give latitude.\n'))
            tic = time()
            cursor = raw.aggregate([{'$match': {'t': {'$gte':tmin, '$lte':tmax}}},
                        {'$geoNear':{'near':{'type':'Point', 'coordinates':[lon, lat]},
                        'distanceField': 'dist.calculated', 'includeLocs': 'dist.location',
                        'spherical': 'true'}}, {'$limit': k}]) 
            print('Aggregation ended in', str(round(time()-tic, 5)), 'seconds')
            #----------------------------------------------------------------------------------------------------------
            file_name = input('Give file name.\n')
            reporting_points(file_name, cursor)
            #----------------------------------------------------------------------------------------------------------
            flag = True
            while flag:
                gate = int(input('0 : k-nearest neighbors\n1 : main menu\n'))
                if gate ==0 or gate == 1:
                    flag = False
            if gate == 0:
                knn_spatial()
            else:
                menu()
        except:
            print('Error/s occured. Please try again.')
            menu()
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    def circle_spatiotemp():
        try:
            global raw
            radius_miles = int(input('Give radius in miles.\n'))
            tmin = int(input('Give minimum time.\n'))
            tmax = int(input('Give maximum time.\n'))
            lon = float(input('Give longtitude.\n'))
            lat = float(input('Give latitude.\n'))
            tic = time()
            cursor = raw.aggregate([{'$match':{'t':{'$gte':tmin,'$lte' :tmax}}},
                                    {'location':{'$geoWithin':{'$centerSphere':[[lon,lat],radius_miles/3963.2] } }} ] )

            print('Aggregation ended in', str(round(time()-tic, 5)), 'seconds')
            #----------------------------------------------------------------------------------------------------------
            file_name = input('Give file name.\n')
            reporting_points(file_name, cursor)
            #----------------------------------------------------------------------------------------------------------
            flag = True
            while flag:
                gate = int(input('0 : k-nearest neighbors\n1 : main menu\n'))
                if gate ==0 or gate == 1:
                    flag = False
            if gate == 0:
                knn_spatial()
            else:
                menu()
        except:
            print('Error/s occured. Please try again.')
            menu()
    
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    def torus_spatiotemp():
        try:
            global raw
            dismin = int(input('Give minimum distance.\n'))
            dismax = int(input('Give maximum distance.\n'))
            tmin = int(input('Give minimum time.\n'))
            tmax = int(input('Give maximum time.\n'))
            lon = float(input('Give longtitude.\n'))
            lat = float(input('Give latitude.\n'))
            tic = time()
            cursor = raw.aggregate([{'$match':{'t':{'$gte':tmin,'$lte':tmax}}},
                                    {'$geoNear':{'near':{'type':'Point', 'coordinates':[lon, lat]},
                                       'distanceField':'dist.calculated', 'maxDistance':dismax, 'minDistance':dismin,'spherical':'true'}}])

            print('Aggregation ended in', str(round(time()-tic, 5)), 'seconds')
            #----------------------------------------------------------------------------------------------------------
            file_name = input('Give file name.\n')
            reporting_points(file_name, cursor)
            #----------------------------------------------------------------------------------------------------------
            flag = True
            while flag:
                gate = int(input('0 : k-nearest neighbors\n1 : main menu\n'))
                if gate ==0 or gate == 1:
                    flag = False
            if gate == 0:
                knn_spatial()
            else:
                menu()
        except:
            print('Error/s occured. Please try again.')
            menu()
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    def triangle_spatioptemp():
        try:
            global raw
            tmin = int(input('Give minimum time.\n'))
            tmax = int(input('Give maximum time.\n'))
            lon1 = float(input('Give first longtitude.\n'))
            lat1 = float(input('Give first latitude.\n'))
            
            lon2 = float(input('Give second longtitude.\n'))
            lat2 = float(input('Give second latitude.\n'))
            
            lon3 = float(input('Give third longtitude.\n'))
            lat3 = float(input('Give third latitude.\n'))
            tic = time()
            cursor = raw.aggregate([{'$match':{'t':{'$gte':tmin,'$lte':tmax}}},
                          {'$match':{'location':{'$geoWithin':{'$geometry':{'type': 'Polygon' ,
                                                                            'coordinates': [ [ [lon1,lat1],[lon2,lat2],[lon3,lat3]]]}}}}}]) 

            print('Aggregation ended in', str(round(time()-tic, 5)), 'seconds')
            #----------------------------------------------------------------------------------------------------------
            file_name = input('Give file name.\n')
            reporting_points(file_name, cursor)
            #----------------------------------------------------------------------------------------------------------
            flag = True
            while flag:
                gate = int(input('0 : k-nearest neighbors\n1 : main menu\n'))
                if gate ==0 or gate == 1:
                    flag = False
            if gate == 0:
                knn_spatial()
            else:
                menu()
        except:
            print('Error/s occured. Please try again.')
            menu()
            
    #OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
            
    def trajectory():
        query_type = int(input('1 : k-most similar\n2 : Overlapping Trajectories\n'))
        if query_type == 1:
            k_most_similar()
        elif query_type == 2:
            overlapping()
        else:
            flag = True
            while flag:
                kappa = int(input('Invalid input, give 0 to main menu or 1 to trajectory menu\n'))
                if kappa == 0 or kappa == 1:
                    flag = False
            if kappa == 0:
                menu()
            else:
                trajectory()

    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    
    def k_most_similar():
        try:
            if 1==1:
                global traj
                mmsi_target = int(input('Give vessel\'s MMSI\n'))
                print(999)
                k = int(input('Give k.\n'))
                print(16)
                tic = time()
                print(1)
                
                cur = traj.find({'mmsi' : mmsi_target})
                mmsiList = traj.distinct('mmsi')
                tra = []
                times = []
                print(666)
                for i in cur:
                    for j in i['pos']:
                        times.append(j['time'])
                        tra.append(j['loc']['coordinates'])
                lcssVals = []
                maxSim = -1
                othersTra = []
                othersTime = []
                lista_twn_mmsis = []
                print(666)
                for mmsi in mmsiList:
                    temp_tra = []
                    temp_time = []
                    if mmsi!=mmsi_target:
                        lista_twn_mmsis.append(mmsi)
                        temp_cur = traj.find({'mmsi':mmsi})
                        for i in temp_cur:
                            for j in i['pos']:
                                temp_time.append(j['time'])
                                temp_tra.append(j['loc']['coordinates'])
                        othersTra.append(temp_tra)
                        othersTime.append(temp_time)
                        lcssVals.append(lcss(temp_tra, tra))
                print('Aggregation ended in', str(round(time()-tic, 5)), 'seconds')
                #----------------------------------------------------------------------------------------------------------
                lcssVals = np.asarray(lcssVals)
                ind = np.argpartition(lcssVals, -k)[-k:]
                Map = folium.Map()

                for i in range(len(tra)):
                    folium.CircleMarker(radius=3, fill=True, location=[tra[i][1],tra[i][0]], popup=mmsi_target,tooltip=str(datetime.fromtimestamp(times[i])), color="red").add_to(Map)

                col = ['blue','green','white','orange','cadetblue','darkred','lightred','beige','darkblue','darkgreen','darkpurple','purple','pink','lightblue','lightgreen','gray','black','lightgray']

                lol = 0
                for tr in ind:
                    for i in range(len(othersTra[tr])):
                        folium.CircleMarker(radius=1, fill=False,location=[othersTra[tr][i][1],othersTra[tr][i][0]], popup=lista_twn_mmsis[lol],tooltip=str(datetime.fromtimestamp(othersTime[tr][i])), color=col[lol]).add_to(Map)
                    lol+=1
                Map.save('Map_trajectories.html')
                #----------------------------------------------------------------------------------------------------------
                flag = True
                while flag:
                    gate = int(input('0 : k-most similar\n1 : main menu\n'))
                    if gate ==0 or gate == 1:
                        flag = False
                if gate == 0:
                    k_most_similar()
                else:
                    menu()
        except:
            print('Error/s occured. Please try again.')
            trajectory()
            
    #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

    def poly(traj, epsilon=.00005):
        tra_plus = [(traj[i][0]+epsilon, traj[i][1]-epsilon) for i in range(len(traj))]
        tra_minus = [(traj[i][0]-epsilon, traj[i][1]+epsilon) for i in range(len(traj))]
        polygon = []
        for i in range(len(traj)):
            polygon.append(tra_plus[i])
        for i in range(len(traj)):
            polygon.append(tra_minus[-i])
        return polygon

    def overlapping():
        try:
            global traj
            mmsi_target = int(input('Give vessel\'s mmsi\n'))

            tic = time()
            cur = traj.find({'mmsi' : mmsi_target})
            mmsiList = traj.distinct('mmsi')
            tra = []
            for i in cur:
                for j in i['pos']:
                    tra.append(j['loc']['coordinates'])

            polygon_given = poly(tra)

            allTheOtherGons = []
            allTheOtherTrajectories = []
            lista_twn_mmsis = []
            for mmsi in mmsiList:
                temp_tra = []
                temp_time = []
                if mmsi!=mmsi_target:
                    lista_twn_mmsis.append(mmsi)
                    temp_cur = traj.find({'mmsi':mmsi})
                    for i in temp_cur:
                        for j in i['pos']:
                            temp_tra.append(j['loc']['coordinates'])
                    allTheOtherTrajectories.append(temp_tra)
                    allTheOtherGons.append(poly(temp_tra))

            GonsKept = []
            for i in range(len(allTheOtherGons)):
                if Polygon(polygon_given).intersects(Polygon(allTheOtherGons[i])):GonsKept.append(allTheOtherTrajectories[i])

            print('Aggregation ended in', str(round(time()-tic, 5)), 'seconds')
            #----------------------------------------------------------------------------------------------------------
            Map = folium.Map()
            for i in range(len(polygon_given)):
                folium.CircleMarker(radius=3, fill=True, location=[polygon_given[i][1],polygon_given[i][0]], popup=mmsi_target, color="blue").add_to(Map)



            for j in range(20):
                for i in range(len(GonsKept[j])):
                    folium.CircleMarker(radius=1, fill=False, location=[GonsKept[j][i][1],GonsKept[j][i][0]],popup=lista_twn_mmsis[j], color="red").add_to(Map)


            Map.save('Map_tra_overlap.html')        
            #----------------------------------------------------------------------------------------------------------
            flag = True
            while flag:
                gate = int(input('0 : overlapping trajectories\n1 : main menu\n'))
                if gate ==0 or gate == 1:
                    flag = False
            if gate == 0:
                overlapping()
            else:
                menu()
        except:
            print('Error/s occured. Please try again.')
            trajectory()    



    
    #OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO

    menu()

except pmg.errors.ConnectionFailure:
    print("Server not available")

