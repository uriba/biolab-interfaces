module Biolab.Interfaces.MySql (
    DbReadable(..),
    ExpDesc(..),
    PlateDesc(..),
    DbMeasurement(..),
    WellDesc(..),
    SelectCriteria(..),
    SampleQuery (..),
    readTable,
    dbConnectInfo,
    mesFromDB,
    loadExpDataDB,
    fromNullString,
    loadMes,
    )
where
import Database.HDBC.MySQL
import Database.HDBC
import Data.ByteString.UTF8 (toString)
import Data.Function (on)
import Data.Maybe (fromMaybe)
import Data.DateTime (fromSeconds, DateTime)
import Data.Char (ord)
import Biolab.Types
import Data.List (find, nub, sort, intercalate, group)
import Control.Monad.Error (runErrorT)
import Control.Monad (join)
import Control.Monad.IO.Class (liftIO)
import Data.Either.Unwrap (fromRight)
import Data.ConfigFile (emptyCP, readfile, get)
import qualified Data.Vector as V
import qualified Data.Map as M

type ExpId = String

data SampleQuery = SampleQuery {sqExpId :: [String], sqPlate :: [Int], sqWell :: [Well] } deriving (Eq, Ord, Show, Read)

sampleQueryToSC :: SampleQuery -> SelectCriteria
sampleQueryToSC (SampleQuery {sqExpId =seid, sqPlate =sp, sqWell =sw})
    | null seid && null sp && null sw = SelectCriteria "" []
    | otherwise = SelectCriteria wc vals
    where
        vals = map toSql seid ++ map toSql sp ++ concatMap wellToSql sw
        wc = "WHERE ( " ++ (intercalate " ) AND ( " . filter (not . null) $ [eid,ps,ws]) ++ " ) "
        eid = intercalate " OR " (replicate (length seid) " exp_id = ? ")
        ps = intercalate " OR " (replicate (length sp) " plate = ? ")
        ws = intercalate " OR " (replicate (length sw) " ( col = ? AND row = ? ) ")

wellFromInts :: Int -> Int -> Well
wellFromInts r c = Well { wRow = ['a'..'h'] !! r, wColumn = c + 1 }

wellToSql :: Well -> [SqlValue]
wellToSql w = [toSql $ wColumn w - 1, toSql $ ((-) `on` ord) (wRow w) 'a']

-- consider adding table names to configuration file as well.
dbConnectInfo :: FilePath -> IO MySQLConnectInfo
dbConnectInfo cf = do
    rv <- runErrorT $
        do
            cp <- join $ liftIO $ readfile emptyCP cf
            let sect = "MYSQL"
            host <- get cp sect "host"
            user <- get cp sect "user"
            passwd <- get cp sect "password"
            dbname <- get cp sect "dbname"
            port <- get cp sect "port"
            unixsock <- get cp sect "unixsocket"
            return $ MySQLConnectInfo {
                mysqlHost = host,
                mysqlUser = user,
                mysqlPassword = passwd,
                mysqlDatabase = dbname,
                mysqlPort = port,
                mysqlUnixSocket = unixsock,
                mysqlGroup = Nothing
            }
    return $ fromRight rv

maxVal = 70000 -- this hack will need resolving in the future...

data SelectCriteria = SelectCriteria {scWhere :: String, scVals :: [SqlValue]}
    deriving (Show)

class DbReadable a where
    dbRead :: [SqlValue] -> a

data ExpDesc = ExpDesc {edExp :: ExpId, edDesc :: String} deriving (Show)

instance DbReadable ExpDesc where
    dbRead [SqlByteString exp_id, SqlByteString desc] = 
        ExpDesc {
            edExp = toString exp_id,
            edDesc = toString desc
        }

data WellDesc = WellDesc {wdExp :: ExpId, wdPlate :: Int, wdWell :: Well, wdDesc :: String} deriving (Show, Eq)

instance DbReadable WellDesc where
    dbRead [SqlByteString exp_id, SqlInt32 p, SqlInt32 row, SqlInt32 col, SqlByteString desc] = 
        WellDesc {
            wdExp = toString exp_id,
            wdPlate = fromIntegral p,
            wdWell = (wellFromInts `on` fromIntegral) row col,
            wdDesc = toString desc
        }

data PlateDesc = PlateDesc {pdExp :: ExpId, pdPlate :: Int, pdDesc :: String, pdOwner :: Maybe String, pdProject :: Maybe String} deriving (Show)

fromNullString :: SqlValue -> Maybe String
fromNullString SqlNull = Nothing
fromNullString (SqlByteString s) = Just . toString $ s

instance DbReadable PlateDesc where
    dbRead [SqlByteString exp_id, SqlInt32 p, SqlByteString desc, owner, project] =
        PlateDesc {
            pdExp = toString exp_id,
            pdPlate = fromIntegral p,
            pdDesc = toString desc,
            pdOwner = fromNullString owner,
            pdProject = fromNullString project
        }

data DbMeasurement = DbMeasurement { dbmExpDesc :: ExpId, dbmPlate :: Int, dbmTime :: DateTime, dbmType :: String, dbmWell :: Well, dbmVal :: Double } deriving (Eq, Show)

instance DbReadable DbMeasurement where
    dbRead [SqlByteString exp_id, SqlInt32 plate_num, SqlByteString mt, SqlInt32 row, SqlInt32 col, SqlInt32 timestamp, v] =
        DbMeasurement {
            dbmExpDesc = toString exp_id,
            dbmPlate = fromIntegral plate_num,
            dbmTime = fromSeconds . fromIntegral $ timestamp,
            dbmType = toString mt,
            dbmWell = well,
            dbmVal = val v
            }
                where
                    val (SqlDouble x) = if x == 0 then maxVal else x
                    val SqlNull = maxVal
                    well = (wellFromInts `on` fromIntegral) row col

readTable :: (DbReadable a) => MySQLConnectInfo -> String -> Maybe SelectCriteria -> IO [a]
readTable db_conf t_name msc = do
    conn <- connectMySQL db_conf
    let where_clause = fromMaybe "" . fmap scWhere $ msc
    let where_params = fromMaybe [] . fmap scVals $ msc
    entries <-  quickQuery' conn ("SELECT * FROM " ++ t_name ++ " " ++ where_clause) where_params
    return . map dbRead $ entries

loadExpDataDB :: FilePath -> ExpId -> Int -> IO ExpData
loadExpDataDB cf exp_id p = do
    db_conf <- dbConnectInfo cf
    readings <- readTable db_conf "tecan_readings" (Just $ SelectCriteria "where exp_id = ? AND plate = ?" [toSql exp_id, toSql p])
    well_labels <- readTable db_conf "tecan_labels" (Just $ SelectCriteria "where exp_id = ? AND plate = ?" [toSql exp_id, toSql p])
    return . makeExpData well_labels $ mesFromDB readings

makeExpData :: [WellDesc] -> [(SampleId,[ColonyMeasurements RawMeasurement])] -> ExpData
makeExpData ws ss = M.fromList [ (l, l_samples l) | l <- labels]
    where
        labels = nub . map wdDesc $ ws
        l_samples l = M.fromList [ (s,fromMaybe (not_found s) . lookup s $ ss) | s <- l_ids l]
        l_ids l = map wdTosid . filter ((l ==) . wdDesc) $ ws
        not_found s = error $ "couldn't find id:" ++ show s ++ " in measurements"

wdTosid :: WellDesc -> SampleId
wdTosid wd = SampleId {sidExpId = wdExp wd, sidPlate = wdPlate wd, sidWell = wdWell wd}

dbMesSampleId :: DbMeasurement -> SampleId
dbMesSampleId m = SampleId {sidExpId = dbmExpDesc m, sidPlate = dbmPlate m, sidWell = dbmWell m}

dbMesType :: DbMeasurement -> MesType
dbMesType (DbMeasurement {dbmType = mt})
    | mt == "YFP" = Fluorescence 1 2
    | mt == "CFP" = Fluorescence 3 4
    | mt == "RFP" = Fluorescence 5 6
    | mt == "MCHERRY" = Fluorescence 5 6
    | mt == "OD600" = Absorbance 600
    | mt == "OD" = Absorbance 600
    | otherwise = error $ "don't know how to deal with measurment of type:" ++ mt

-- assumes all measurements are of the same ColonyId
samples :: [DbMeasurement] -> [ColonyMeasurements RawMeasurement]
samples dbms = zipWith ($) (map mes $ mts dbms) (repeat dbms)
    where
        mts = nub . map dbMesType
        mes mt = binDbMes mt . filter ((mt ==) . dbMesType)

-- assumes all measurements are of the same ColonyId and have the same type (checked below).
binDbMes :: MesType -> [DbMeasurement] -> ColonyMeasurements RawMeasurement
binDbMes (Absorbance a) dbm = AbsorbanceMeasurement (AbsorbanceSample {asWaveLength = a, asMes = rawMes dbm})
binDbMes (Fluorescence a b) dbm = FluorescenseMeasurement (FluorescenseSample {flExcitation = a, flEmission = b, flMes = rawMes dbm})
binDbMes (Luminesense a) dbm = LuminesenseMeasurement (LuminescenseSample {lsWaveLength = a, lsMes = rawMes dbm})

rawMes :: [DbMeasurement] -> RawColonyMeasurements
rawMes dbm
    | single_type dbm && single_colony dbm = V.fromList . sort . map (\x -> (dbmTime x, RawMeasurement . dbmVal $ x)) $ dbm
    | otherwise = error $ "mesurements of multiple types/colonies: " ++ show dbm
    where
        single_type = (1 ==) . length . group . map dbmType
        single_colony = (1 ==) . length . group . map dbMesSampleId

colonySamples :: SampleId -> [DbMeasurement] -> [DbMeasurement]
colonySamples sid = filter ((sid ==) . dbMesSampleId)

mesFromDB :: [DbMeasurement] -> [(SampleId,([ColonyMeasurements RawMeasurement]))]
mesFromDB dbms = [ (sid, samples . colonySamples sid $ dbms) | sid <- sids dbms]
    where
        sids = nub . map dbMesSampleId

loadMes :: MySQLConnectInfo -> SampleQuery -> IO [(SampleId,[ColonyMeasurements RawMeasurement])]
loadMes db_conf sq = fmap mesFromDB $ readTable db_conf "tecan_readings" (Just $ sampleQueryToSC sq)
