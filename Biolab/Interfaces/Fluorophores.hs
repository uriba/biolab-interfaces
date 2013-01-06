module Biolab.Interfaces.Fluorophores (
    flVals,
)
where

import Biolab.Types (MesType(..))

flVals :: String -> MesType
flVals mt
    | mt == "YFP" = Fluorescence 1 2
    | mt == "CFP" = Fluorescence 3 4
    | mt == "RFP" = Fluorescence 5 6
    | mt == "MCHERRY" = Fluorescence 5 6
    | mt == "OD600" = Absorbance 600
    | mt == "OD" = Absorbance 600
    | otherwise = error $ "don't know how to deal with measurment of type:" ++ mt
