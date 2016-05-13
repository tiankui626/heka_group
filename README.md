# hekad extention

a heka extention to group data and format the data as influxdb lines

# params
    * tags: influxdb tags fielname, seperated by " "
    * groups: influxdb values fiels, seperated by " "
    * value: counter value 
    * ticker_interval: merge interval
    * logger: logger name
    * serie_name: influxdb seria name
    * only_province: private uses
    * debug: private uses
```
2016/05/03 15:39:24
:Timestamp: 2016-04-28 15:38:59 +0000 UTC
:Type: PushLogfile
:Hostname: test2
:Pid: 0
:Uuid: 1d96d469-6c88-4934-bde2-f71ec55eda70
:Logger: ssp
:Payload: 2016-04-28 15:38:59 32 106751 3129928 161 VyG-E3iwmzchRFM_ fa38fa660f57c1f5 1156310000 3dca1b38f0754161948d80d6c568d676 30 2160 2100 200 1 0 1 6 2100 4

:EnvVersion:
:Severity: 7
:Fields:
    | name:"PmpId" type:string value:"3dca1b38f0754161948d80d6c568d676"
    | name:"PlatForm" type:string value:"32"
    | name:"AdserverId" type:string value:"VyG-E3iwmzchRFM_"
    | name:"Channel" type:string value:"161"
    | name:"Slot" type:string value:"106751"
    | name:"City" type:string value:"1156310000"
    | name:"Vid" type:string value:"3129928"
    | name:"Uid" type:string value:"fa38fa660f57c1f5"


AdserverId,Hostname=test2 value="VyG-E3o9EWD3rPkc" 1461857939
Slot,Hostname=test2 value="106751" 1461857939
PlatForm,Hostname=test2 value="32" 1461857939
Vid,Hostname=test2 value="3111479" 1461857939
PlatForm,Hostname=test2 value="32" 1461857939
City,Hostname=test2 value="1156310000" 1461857939
Slot,Hostname=test2 value="106751" 1461857939
PmpId,Hostname=test2 value="f11f8cd10bd4461f86884367fb222903" 1461857939
AdserverId,Hostname=test2 value="VyG-E3o9EWD3rPkc" 1461857939
Channel,Hostname=test2 value="29,34" 1461857939
Uid,Hostname=test2 value="865188022658857" 1461857939
Vid,Hostname=test2 value="61702" 1461857938
PlatForm,Hostname=test2 value="32" 1461857938
City,Hostname=test2 value="1156310000" 1461857938
Slot,Hostname=test2 value="106752" 1461857938
PmpId,Hostname=test2 value="28e4f5e7ec1d4b79880d3a2928eea1e9" 1461857938
AdserverId,Hostname=test2 value="VyG-EsmA8nmSatss" 1461857938
Uid,Hostname=test2 value="357947040310766" 1461857938
Channel,Hostname=test2 value="29,28,27,36" 1461857938
Vid,Hostname=test2 value="3129928" 1461857939
Uid,Hostname=test2 value="fa38fa660f57c1f5" 1461857939
City,Hostname=test2 value="1156310000" 1461857939
Channel,Hostname=test2 value="161" 1461857939
PmpId,Hostname=test2 value="3dca1b38f0754161948d80d6c568d676" 1461857939
AdserverId,Hostname=test2 value="VyG-E3iwmzchRFM_" 1461857939
Slot,Hostname=test2 value="106751" 1461857939
PlatForm,Hostname=test2 value="32" 1461857939
```
