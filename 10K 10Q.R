###  Record checks built at the end, but no looping built into the code - yet - to avoid over-adding.
##  
##
##    To reduce records impacts only NUM values from the End of Quarter date forward 
##    will be integrated. 

# 10-Q & 10-K filings
#
#  if tag is custom (version=adsh), 0 if it is standard. 
#Note: This flag is technically redundant with the version and adsh fields.
#
# This loads the datasets into a database.
# https://www.sec.gov/dera/data/financial-statement-and-notes-data-set.html
#
#  SIC code list https://www.sec.gov/info/edgar/siccodes.htm
# 
# # The Financial Statement and Notes Data Sets below provide the text and detailed numeric 
# information from all financial statements and their notes.  This data is extracted from exhibits 
# to corporate financial reports filed with the Commission using eXtensible Business Reporting Language (XBRL).
# 
# The data is Tagsented in a flattened format to help users analyze and compare corporate disclosure information 
# over time and across registrants. The data sets also contain additional fields such as a company's Standard Industrial 
# Classification to facilitate the data's use.
# 
# These data sets will be updated quarterly. Data contained in documents filed after 5:30PM Eastern on the 
# last business day of a quarter will be included in the subsequent quarterly posting.
# 
# NUM file does not like getting loaded into the database so that's the only one 
# 
if(!require(plyr)){install.packages("plyr") 
  library(plyr)}
if(!require(dplyr)){install.packages("dplyr")
  library(dplyr)}
if(!require(RODBC)){install.packages("RODBC")
  library(RODBC)}
if(!require(sqldf)){install.packages("sqldf")
  library(sqldf)}
if(!require(stringr)){install.packages("stringr")
  library(stringr)}



  setwd("C:/SEC data/Statement&Notes")
  
  ## syntax for odbcConnect if you haven't built the uid & pwd in the Environmental Variables <- which is preferred.
  ch <- odbcConnect("DatabaseNameInODBCSource", uid = "UserID", pwd = "Password")
  ch <- odbcConnect("Problem")
  

## change file name for quarter & year in three places
    q <-c("q1","q2","q3","q4")
    yr <- 2017
  
  
  quarter <- "2017q4" 
  zipname <-gsub(" ","",paste(quarter,"_notes.zip"))
  
  
  dataset_url <-paste("https://www.sec.gov/files/dera/data/financial-statement-and-notes-data-sets/",zipname)
  dataset_url <- gsub(" ","",dataset_url)
  
  
    ########## Loading ##########
  ###
  ##  Begin unzipping, loading and tidying the various datasets
  ###
  
  ## the files are static, so no need to overwrite 
  ## existing file for fresh data
  if(!file.exists(zipname)){
    download.file(dataset_url, destfile=zipname, mode = 'wb')}
  
  #unzip the dataset & load files into R
  unzip(zipname, overwrite= TRUE)
  
  
  
  ### loading is done by file size from small to large

################################################################################     
## sub table - most import - list of companies which filed & defines the period which
##        will drive which NUMber values are migrated.
################################################################################     

   SECFinDataSub<-read.delim("sub.tsv", sep="\t", header = TRUE, na.strings = "", quote = "", stringsAsFactors = FALSE  )
   SECFinDataSub<-merge(quarter,SECFinDataSub)
   names(SECFinDataSub)[names(SECFinDataSub)=="x"] <- "qtrSrc"
   
   #create a "clean" unique name for joining with other tables
   ## Normalize the name by removing punctuation and blank spaces and make everything upper case
   SECFinDataSub <-mutate(SECFinDataSub, CleanName = str_replace_all(SECFinDataSub$name, "[[:punct:]]", ""))
   SECFinDataSub$CleanName <-str_replace_all(SECFinDataSub$CleanName, " ", "")
   SECFinDataSub$CleanName <-toupper(SECFinDataSub$CleanName)
 
   
   ddate <- sqldf("select distinct period from SECFinDataSub")
   query1 <-paste0("select * from SECFinDataNum where ddate = '",ddate,"'")
   
   
   
   
   
################################################################################         
#
#     num table - has the date which will delimit
#
################################################################################  
   
   SECFinDataNum<-read.delim("./num.tsv", sep="\t", header = TRUE, na.strings = "", quote = "", stringsAsFactors = FALSE  )
   #  get max characters to check   
   #max(na.omit(as.data.frame(nchar(SECFinDataNum$coreg))))
   
    SECFinDataNum<-mutate(SECFinDataNum, 
                          footnote2=substr(SECFinDataNum$footnote,255,509),
                          footnote3=substr(SECFinDataNum$footnote,510,764)
    )
   SECFinDataNum<-merge(quarter,SECFinDataNum)
   names(SECFinDataNum)[names(SECFinDataNum)=="x"] <- "qtrSrc"

   
################################################################################       
###       dim table 
################################################################################     
   
  SECFinDataDim<-read.delim("dim.tsv", sep="\t", header = TRUE, na.strings = "", quote = "", stringsAsFactors = FALSE )
  # SECFinDataDim<-mutate(SECFinDataDim, 
  #                       segments2=substr(SECFinDataDim$segments,255,509),
  #                       segments3=substr(SECFinDataDim$segments,510,764),
  #                       segments4=substr(SECFinDataDim$segments,765,1019),
  #                       segments5=substr(SECFinDataDim$segments,1020,1274))
  SECFinDataDim<-merge(quarter,SECFinDataDim)
  names(SECFinDataDim)[names(SECFinDataDim)=="x"] <- "qtrSrc"
                                            
  #  get max characters to check   
      # max(na.omit(as.data.frame(nchar(SECFinDataDim$segments))))

################################################################################  
##  ren table
################################################################################     
   
    SECFinDataRen<-read.delim("ren.tsv", sep="\t", header = TRUE, na.strings = "", quote = "", stringsAsFactors = FALSE  )
    #  get max characters to check   
   # max(na.omit(as.data.frame(nchar(SECFinDataRen$longname))))
    # SECFinDataRen<-mutate(SECFinDataRen, 
    #                       shortname2=substr(SECFinDataRen$shortname,255,509),
    #                       longname2=substr(SECFinDataRen$longname,255,509))
    SECFinDataRen<-merge(quarter,SECFinDataRen)
    names(SECFinDataRen)[names(SECFinDataRen)=="x"] <- "qtrSrc"
    
    
    #max(na.omit(as.data.frame(nchar(SECFinDataRen$report))))
    
    
################################################################################         
#     cal table
################################################################################     
    
    SECFinDataCal<-read.delim("./cal.tsv", sep="\t", header = TRUE, na.strings = "", quote = "", stringsAsFactors = FALSE  )
    SECFinDataCal<-merge(quarter,SECFinDataCal)
    names(SECFinDataCal)[names(SECFinDataCal)=="x"] <- "qtrSrc"
     
################################################################################         
#     tag table
################################################################################     
    
    SECFinDataTag<-read.delim("./tag.tsv", sep="\t", header = TRUE, na.strings = "", quote = "", stringsAsFactors = FALSE  )
    # SECFinDataTag<-mutate(SECFinDataTag, 
    #                       doc2=substr(SECFinDataTag$doc,255,509),
    #                       doc3=substr(SECFinDataTag$doc,510,764),
    #                       doc4=substr(SECFinDataTag$doc,765,1019),
    #                       doc5=substr(SECFinDataTag$doc,1020,1274),
    #                       doc6=substr(SECFinDataTag$doc,1275,1529),
    #                       doc7=substr(SECFinDataTag$doc,1530,1784),
    #                       doc8=substr(SECFinDataTag$doc,1785,2039),
    #                       doc9=substr(SECFinDataTag$doc,2040,2294),
    #                       tlabel2=substr(SECFinDataTag$tlabel,255,509)
    #                         )
    SECFinDataTag<-merge(quarter,SECFinDataTag)
    #  get max characters to check   
   # max(na.omit(as.data.frame(nchar(SECFinDataTag$doc))))
    names(SECFinDataTag)[names(SECFinDataTag)=="x"] <- "qtrSrc"
    
################################################################################         
#     txt table
################################################################################
    
    SECFinDataTxt<-read.delim("txt.tsv", sep="\t", header = TRUE, na.strings = "", quote = "", stringsAsFactors = FALSE  )
    #  get max characters to check   
     # max(na.omit(as.data.frame(nchar(SECFinDataTxt$value))))
    
    # SECFinDataTxt<-mutate(SECFinDataTxt, 
    #                       footnote2=substr(SECFinDataTxt$footnote,255,509),
    #                       footnote3=substr(SECFinDataTxt$footnote,510,764),
    #                       
    #                       value2=substr(SECFinDataTxt$value,255,509),
    #                       value3=substr(SECFinDataTxt$value,510,764),
    #                       value4=substr(SECFinDataTxt$value,765,1019),
    #                       value5=substr(SECFinDataTxt$value,1020,1274),
    #                       value6=substr(SECFinDataTxt$value,1275,1529),
    #                       value7=substr(SECFinDataTxt$value,1530,1784),
    #                       value8=substr(SECFinDataTxt$value,1785,2039),
    #                       value9=substr(SECFinDataTxt$value,2040,2294)
    #                   )
    SECFinDataTxt<-merge(quarter,SECFinDataTxt)
    names(SECFinDataTxt)[names(SECFinDataTxt)=="x"] <- "qtrSrc"
    
################################################################################         
#
#     pre table
#
################################################################################         
   
    SECFinDataPre<-read.delim("./Pre.tsv", sep="\t", header = TRUE, na.strings = "", quote = "", stringsAsFactors = FALSE  )
  
    #  get max characters to check   
      #max(na.omit(as.data.frame(nchar(SECFinDataPre$plabel))))
    # SECFinDataPre<-mutate(SECFinDataPre, 
    #                       plabel2=substr(SECFinDataPre$plabel,255,509),
    #                       plabel3=substr(SECFinDataPre$plabel,510,764)
    #           )
    SECFinDataPre<-merge(quarter,SECFinDataPre)
    names(SECFinDataPre)[names(SECFinDataPre)=="x"] <- "qtrSrc"
#############################################################################
##
##  save files to database
##
#############################################################################

     ch <- odbcConnect("Azure-VizzySolutions", uid = "viZZyAdmin", pwd = "This is Azure 1$t A!!3mpt.9")    
    # max(na.omit(as.data.frame(nchar(SECFinDataCal$pversion))))
    
## Cal ###  
    x<-Sys.time()
    
     write.table(SECFinDataSub, file = "SECFinDataSub.txt", quote = FALSE, sep = "\t", row.names = FALSE)
     write.table(SECFinDataRen, file = "SECFinDataRen.txt", quote = FALSE, sep = "\t", row.names = FALSE)
     write.table(SECFinDataDim, file = "SECFinDataDim.txt", quote = FALSE, sep = "\t", row.names = FALSE)
     write.table(SECFinDataNum, file = "SECFinDataNum.txt", quote = FALSE, sep = "\t", row.names = FALSE)
     write.table(SECFinDataCal, file = "SECFinDataCal.txt", quote = FALSE, sep = "\t", row.names = FALSE)
     write.table(SECFinDataTag, file = "SECFinDataTag.txt", quote = FALSE, sep = "\t", row.names = FALSE)
     write.table(SECFinDataTxt, file = "SECFinDataTxt.txt", quote = FALSE, sep = "\t", row.names = FALSE)
     write.table(SECFinDataPre, file = "SECFinDataPre.txt", quote = FALSE, sep = "\t", row.names = FALSE)
     
    
    
    
    
    sqlSave(ch,SECFinDataCal, tablename = "TempT", rownames = FALSE)
    print(Sys.time()-x)
    sqlQuery(ch, "Insert into SECFinDataCal select * from TempT")
    Sys.time()
    sqlQuery(ch,"Drop table TempT")  
    
## Dim ###    
    
    sqlSave(ch,SECFinDataDim, tablename = "TempT", rownames = FALSE)
    sqlQuery(ch, "insert into SECFinDataDim
            select x as qtrSrc,Dimhash, 
              CONCAT(segments, segments2, segments3, segments4, segments5) as segments, segt 
           from TempT")
    sqlQuery(ch,"Drop table TempT")  
    
## Num ###
    
    sqlSave(ch,SECFinDataNum, tablename = "TempT", rownames = FALSE)
    ##sqlQuery("Insert into SECFinDataNum select * from Temp")
    sqlQuery(ch, "insert into SECFinDataNum
            select 
            x as qtrSrc, adsh, tag, version, ddate, qtrs, uom, dimh, iprx, value, 
            concat(footnote, footnote2, footnote3) as footnote, 
            footlen, dimn, coreg, durp, datp, dcml
            from TempT")
     sqlQuery(ch,"Drop table TempT")  
    
## Pre ### 
     
    sqlSave(ch,SECFinDataPre, tablename = "TempT", rownames = FALSE)
    sqlQuery(ch, "insert into SECFinDataPre
             select 
             x as qtrSrc, adsh, report, line, stmt, inpth, tag, version, prole, 
             concat(plabel, plabel2, plabel3) as plabel, 
             negating
             from TempT")
     sqlQuery(ch,"Drop table TempT")  

## Ren ###
     
     sqlSave(ch,SECFinDataRen, tablename = "TempT", rownames = FALSE)
     ##sqlQuery("Insert into SECFinDataRen select * from Temp")
     sqlQuery(ch, "insert into SECFinDataRen
              select 
              x as qtrSrc,
              adsh, report, rfile, menucat,
              CONCAT(shortname, shortname2) as shortname, 
              CONCAT(longname, longname2) as longname,  
              roleuri,parentroleuri, parentreport,	ultparentrpt
              from TempT")
    sqlQuery(ch,"Drop table TempT")  
    
## Sub ### 
    
    sqlSave(ch,SECFinDataSub, tablename = "TempT", rownames = FALSE)
    sqlQuery(ch, "insert into SECFinDataSub select * from TempT")
    sqlQuery(ch,"drop table TempT ")
     
## Tag ###
    
    sqlSave(ch,SECFinDataTag, tablename = "TempT", rownames = FALSE)
    ##sqlQuery("Insert into SECFinDataTag select * from Temp")
    sqlQuery(ch, "insert into SECFinDataTag
             select x as qtrSrc, tag, version, custom, abstract, datatype, iord, crdr,
             concat(tlabel, tlabel2) as tlabel,
             CONCAT(doc, doc2, doc3, doc4, doc5, doc6, doc7, doc8, doc9) as doc from TempT")
     sqlQuery(ch,"Drop table TempT")  
    
## Txt ###
     
    sqlSave(ch,SECFinDataTxt, tablename = "TempT", rownames = FALSE)
    ##sqlQuery("Insert into SECFinDataTxt select * from Temp")
    sqlQuery(ch, "insert into SECFinDataTxt
                  select 
             x as qtrSrc, adsh, tag, version, ddate, qtrs, iprx, lang, dcml, durp, datp, dimh, dimn, coreg, escaped, srclen, txtlen, 
             concat(footnote,footnote2, footnote3) as footnote, 
             footlen, context, 
             concat(value, value2, value3, value4, value5, value6, value7, value8, value9) as value
             from TempT")
    
    sqlQuery(ch,"Drop table TempT")  
    
    
        
    
### Make sure all the records made it.  Get the expected number of records
  
    
    Cal<-cbind("SECFinDataCal",sum(!is.na(SECFinDataCal$x)))
    Dim<-cbind("SECFinDataDim",sum(!is.na(SECFinDataDim$x)))
    Num<-cbind("SECFinDataNum",sum(!is.na(SECFinDataNum$x)))
    Pre<-cbind("SECFinDataPre",sum(!is.na(SECFinDataPre$x)))
    Ren<-cbind("SECFinDataRen",sum(!is.na(SECFinDataRen$x)))
    Sub<-cbind("SECFinDataSub",sum(!is.na(SECFinDataSub$x)))
    Tag<-cbind("SECFinDataTag",sum(!is.na(SECFinDataTag$x)))
    Txt<-cbind("SECFinDataTxt",sum(!is.na(SECFinDataTxt$x)))
    
## combine expected records into a dataframe & manipulate the datatypes    
    FCts <-as.data.frame(rbind(Cal,Dim,Num,Pre,Ren, Sub, Tag, Txt))
    FCts<-merge(quarter, FCts)
    colnames(FCts)<-c("Quarter","FileN","ExpectCt")
    FCts$Quarter<-as.character(FCts$Quarter)
    FCts$FileN<-as.character(FCts$FileN)
    FCts$ExpectCt<-as.numeric(as.character(FCts$ExpectCt))
 
## build union query structure to get the counts from the database       
    Cal <- paste0("select 'SECFinDataCal' as FileN, count(qtrSrc) as NewRecCt from SECFinDataCal where qtrSrc = '", quarter,"' group by qtrSrc union all ")
    Dim <- paste0("select 'SECFinDataDim' as FileN, count(qtrSrc) as NewRecCt from SECFinDataDim where qtrSrc = '", quarter,"' group by qtrSrc union all ")
    Num <- paste0("select 'SECFinDataNum' as FileN, count(qtrSrc) as NewRecCt from SECFinDataNum where qtrSrc = '", quarter,"' group by qtrSrc union all ")
    Pre <- paste0("select 'SECFinDataPre' as FileN, count(qtrSrc) as NewRecCt from SECFinDataPre where qtrSrc = '", quarter,"' group by qtrSrc union all ")
    Ren <- paste0("select 'SECFinDataRen' as FileN, count(qtrSrc) as NewRecCt from SECFinDataRen where qtrSrc = '", quarter,"' group by qtrSrc union all ")
    Sub <- paste0("select 'SECFinDataSub' as FileN, count(qtrSrc) as NewRecCt from SECFinDataSub where qtrSrc = '", quarter,"' group by qtrSrc union all ")
    Tag <- paste0("select 'SECFinDataTag' as FileN, count(qtrSrc) as NewRecCt from SECFinDataTag where qtrSrc = '", quarter,"' group by qtrSrc union all ")
    Txt <- paste0("select 'SECFinDataTxt' as FileN, count(qtrSrc) as NewRecCt from SECFinDataTxt where qtrSrc = '", quarter,"' group by qtrSrc")
    
    Call <- paste0(Cal, Dim, Num, Pre, Ren, Sub, Tag, Txt)
    
    NewRec<-sqlQuery(ch,Call)
    NewRec$FileN<-as.character(NewRec$FileN)
    
## Combine Expected and database values into new table.  Check for matches and print result    
    RecCheck <-merge(FCts, NewRec)
    RecCheck <-mutate(RecCheck, Match = if_else(ExpectCt-NewRecCt == 0,"Match",as.character(ExpectCt-NewRecCt)))
    
    
    print(RecCheck)
    x<- dim(RecCheck)
    y<-x[1]
    if(y == 8){"All Records Found"}else {"Records Missing"}
    
    
    
    
    
   
      
    
# Clean Up environment    
  rm(Cal, Dim, Num, Pre, Ren, Sub, Tag, Txt,Call)
 # rm(SECFinDataCal, SECFinDataDim, SECFinDataNum, SECFinDataPre, SECFinDataRen, SECFinDataSub, SECFinDataTag, SECFinDataTxt)
#  rm(NewRec, RecCheck, FCts, dataset_url)
  odbcCloseAll() 
  
  
  
  