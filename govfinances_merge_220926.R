## Justin de Benedictis-Kessner
## Updating 09/26/2022

## This code merges the individual yearly files and historical 1967-2012 file 
## ("Special 60") for the Census of Government finances data gathered by the 
## US Census Bureau and available online: 
## https://www.census.gov/programs-surveys/gov-finances.html


## ------------ ##
#### Preamble ####
## ------------ ##
library(foreign)
library(tidyverse)
library(stringr)
library(zoo)
library(pdftools)
set.seed(02139)


# lead and lag functions that account for missing years
lag.new <- function(x, n = 1L, along_with){
  index <- match(along_with - n, along_with, incomparable = NA)
  out <- x[index]
  attributes(out) <- attributes(x)
  out
}

lead.new <- function(x, n = 1L, along_with){
  index <- match(along_with + n, along_with, incomparable = NA)
  out <- x[index]
  attributes(out) <- attributes(x)
  out
}

## ------------- ##
#### Load Data ####
## ------------- ##

#### Composite old files ####
## (Historical Data Base of Individual Government Finances)
## Files from zip downloaded here: https://www.census.gov/programs-surveys/gov-finances/data/historical-data.html
## This code merges the individual finance files from the historical database  
## This process is slow b/c files are very large, so the below line will toggle this process off if done already
assemble_historical <- FALSE # change to TRUE to re-assemble
if(assemble_historical){
	loc.individualfiscaldata <- "_IndFin_1967-2012/"
	
	IndFin.years <- c(70:99, "00","01", "02", "03", "04", "05", "06", "07", "08", "09","10", "11", "12")
	IndFin <- NULL
	for(i in 1:length(IndFin.years)){
		tempa <- read_csv(file = paste0(loc.individualfiscaldata,"IndFin",IndFin.years[i],"a.Txt"))
		tempb <- read_csv(file = paste0(loc.individualfiscaldata,"IndFin",IndFin.years[i],"b.Txt"))
		tempc <- read_csv(file = paste0(loc.individualfiscaldata,"IndFin",IndFin.years[i],"c.Txt"))
		
		temp <- bind_cols(tempa, tempb, tempc)
		names(temp) <- str_replace_all(names(temp), c(" " = ".", "-" = "", "&" = "", "," = ""))
		temp <- mutate(temp,
									 FYEndDate = as.numeric(FYEndDate),
									 YearofData = as.character(YearofData))
		# temp <- filter(temp, Type.Code %in% c(2,3)) # subset to only city or township with this line
		# temp <- filter(temp, Type.Code %in% c(1,2,3)) # alternatively include counties with this line
		temp <- filter(temp, Type.Code %in% c(1,2,3,5)) # or include school districts too with this line
		IndFin <- bind_rows(IndFin, temp)
		print(IndFin.years[i])
		rm(temp,tempa,tempb,tempc)
	}
	# IndFin$govid_14 <- IndFin$ID
	
	IndFin <- IndFin %>%
		rename("govid_9" = "ID",
					 "fips_state" = "FIPS.CodeState",
					 "Year.of.Pop.Data" = "YearPop",
					 "Total.LTD.Outstanding" = "Total.LTD.Out",
		)
	
	cwalk <- readxl::read_xls("_IndFin_1967-2012/GOVS_ID_to_FIPS_Place_Codes_2002.xls",skip = 16)
	names(cwalk) <- c("govid_9","govid_14","gov_state_code","gov_type_code","gov_county_code",
										"Name","CountyName","fips_state","fips_county","fips_place")
	cwalk_for_merge <- cwalk %>%
	  select(govid_9,govid_14,gov_type_code,fips_county,fips_place)
	IndFin <- left_join(IndFin,cwalk_for_merge,by="govid_9")
	
	
	
	## removing imputed/
	IndFin <- subset(IndFin, YearofData!="CC")
	IndFin <- subset(IndFin, YearofData!="BB")
	IndFin <- subset(IndFin, YearofData!="II")
	
	## adjust column types for merging with newer years:
	IndFin <- IndFin %>%
	  mutate(FYEndDate = str_pad(FYEndDate,width = 4,side = "left",pad = "0"))
	

	save(IndFin, file="IndFin_19672012.RData")
}
if(!assemble_historical){ # if data already assembled
	load(file="IndFin_19672012.RData")
}


## Individual newer yearly files: ------
# yearly files from these pages: https://www.census.gov/data/datasets/2013/econ/local/public-use-datasets.html
## --------- ##
#### 2013: ####
## --------- ##
findat_2013 <- read_fwf("2013_Individual_Unit_file/2013FinEstDAT_10162019modp_pu.txt",
                        col_positions = fwf_widths(widths = c(14,3,12,4,1),
                                                   col_names = c("govid_14","item_code","amount","YearData","imputation"))
)
cwalk_2013 <- read_fwf("2013_Individual_Unit_file/Fin_GID_2013.txt",
                       col_positions = fwf_widths(widths = c(14,64,35,2,3,5,9,2,7,2,2,2,4,2),
                                                  col_names = c("govid_14","Name","CountyName",
                                                                "fips_state","fips_county","fips_place",
                                                                "population","pop_year","enrollment","enrollment_year",
                                                                "function_code","school_lvl_code","FYEndDate","SurvYear")))
findat_2013 <- left_join(findat_2013,cwalk_2013,by="govid_14")

## bring in item_code names from here: https://www.census.gov/govs/www/stateloctechdoc.html#dict
# findat_codes <- read_csv("finances_categories.csv") %>%
# mutate(var_name = str_replace_all(var_name,c(" " = ".", "-" = "", "&" = "", "," = "","\\.\\."=".")))
# findat_2013 <- left_join(findat_2013,findat_codes,by="item_code")
# findat_2013 <- spread(findat_2013, key=var_name, value=amount)
findat_2013 <- findat_2013 %>% select(-imputation)

findat_2013_wide <- spread(findat_2013, key=item_code, value=amount)
# findat_2013_wide <- findat_2013_wide %>%
#   filter(imputation == "R") # eliminate imputed variables
## check that this is right number of governments: 
# length(unique(findat_2013$govid_14[findat_2013$imputation=="R"]))


## Aggregating data into broad categories using "methodology" file downloaded here: https://www.census.gov/data/datasets/2013/econ/local/public-use-datasets.html
# This page also says these summary tabs haven't changed since 2010: https://www.census.gov/programs-surveys/gov-finances/technical-documentation/classification-manuals.html
findat_2013_wide <- findat_2013_wide %>%
  rowwise() %>%
  mutate(Total.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                             # A56, # Charges.Natural.Resources.Forestry missing b/c only at state-fed level
                             A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                             # U21, # missing b/c only at state-fed level
                             U30, U40, U41, U50, U95, U99, A90, A91, A92, A93, A94, X01, X02, X05, X08, Y01, Y02, Y04, Y11, Y12, Y51, Y52,na.rm=T),
         
         Total.Expenditure = sum(E01, E03, E04, E05, E12, E16, E18, E21, E22, E23, E24, E25,
                                 # E26, # Current.Operations.Legislative.Services missing b/c only at state-fed level
                                 E29, E31, E32, E36, E44, E45, E50, E52, 
                                 # E55, # Current.Operations.Natural.Resources.Fish.and.Game missing b/c only at state-fed level
                                 # E56, # Current.Operations.Natural.Resources.Forestry missing b/c only at state-fed level
                                 E59, E60, E61, E62, E66, 
                                 # E73, # missing b/c only at state-fed level
                                 E74, E75, E77, E79, E80, E81, E85, E87, E89, E90, E91, E92, E93, E94, I89, I91, I92, I93, I94, J19, L67, J68, J85, X11, X12, Y05, Y06, Y14, Y53, F01, F03, F04, F05, F12, F16, F18, F21, F22, F23, F24, F25, 
                                 # F26, # Construction.Legislative missing b/c only at state-fed level
                                 F29, F31, F32, F36, F44, F45, F50, F52,
                                 # F55, # Construction.Fish.and.Game missing b/c only at state-fed level
                                 # F56, # Construction.Forestry missing b/c only at state-fed level
                                 F59, F60, F61, F62, F66, F77, F79, F80, F81, F85, F87, F89, F90, F91, F92, F93, F94, G01, G03, G04, G05, G12, G16, G18, G21, G22, G23, G24, G25, 
                                 # G26, # Other.Capital.Outlay.Legislative missing b/c only at state-fed level
                                 G29, G31, G32, G36, G44, G45, G50, G52, 
                                 # G55, # Other.Capital.Outlay.Fish.and.Game missing b/c only at state-fed level
                                 # G56, # Other.Capital.Outlay.Forestry missing b/c only at state-fed level
                                 G59, G60, G61, G62, G66, G77, G79, G80, G81, G85, G87, G89, G90, G91, G92, G93, G94, J67, L01, L04, L05, L12, L18, L23, L25, L29, L32, L36, L44, L50, L52, L59, L60, L61, L62, L66, L67, L79, L80, L81, L87, L89, L91, L92, L93, L94, M01, M04, M05, M12, M18, M21, M23, M24, M25, M29, M30, M32, M36, M44, M50, M52, 
                                 # M55, # Intergovernmental.to.Local.NEC.Fish.and.Game missing b/c only at state-fed level
                                 # M56, # Intergovernmental.to.Local.NEC.Forestry missing b/c only at state-fed level
                                 M59, M60, M61, M62, M66, M67, M68, M79, M80, M81, M87, M89, M91, M92, M93, M94, Q12, Q18, S67, 
                                 # S74, # missing b/c only at state-fed level
                                 S89,na.rm=T),
         
         # exp categories:
         Direct.Expenditure = sum(E01, E03, E04, E05, E12, E16, E18, E21, E22, E23, E24, E25,
                                  # E26, 
                                  E29, E31, E32, E36, E44, E45, E50, E52, 
                                  # E55, E56,
                                  E59, E60, E61, E62, E66, 
                                  # E73,
                                  E74, E75, E77, E79, E80, E81, E85, E87, E89, E90, E91, E92, E93, E94, F01, F03, F04, F05, F12, F16, F18, F21, F22, F23, F24, F25,
                                  # F26, 
                                  F29, F31, F32, F36, F44, F45, F50, F52,
                                  # F55, F56, 
                                  F59, F60, F61, F62, F66, F77, F79, F80, F81, F85, F87, F89, F90, F91, F92, F93, F94, G01, G03, G04, G05, G12, G16, G18, G21, G22, G23, G24, G25, 
                                  # G26, 
                                  G29, G31, G32, G36, G44, G45, G50, G52, 
                                  # G55, G56,
                                  G59, G60, G61, G62, G66, G77, G79, G80, G81, G85, G87, G89, G90, G91, G92, G93, G94, X11, X12, Y05, Y06, Y14, Y53, J19, J67, J68, J85, I89, I91, I92, I93, I94,na.rm=T),
         
         Total.HospitalTotal.Exp = sum(E36, F36, G36,na.rm=T),
         CorrectTotal.Exp = sum(E04, F04, G04, E05, F05, G05,na.rm=T),
         Natural.ResTotal.Exp = sum(
           # E55, F55, G55, E56, F56, G56,
           E59, F59, G59,na.rm=T),
         Total.EducTotal.Exp = sum(E12, F12, G12, E16, F16, G16, E18, F18, G18, J19, E21, F21, G21,na.rm=T),
         General.NECTotal.Exp = sum(E89, F89, G89,na.rm=T), # called "other and unallocable" in methodology doc
         Fire.ProtTotal.Expend = sum(E24, F24, G24,na.rm=T),
         Police.ProtTotal.Exp = sum(E62, F62, G62,na.rm=T),
         HealthTotal.Expend = sum(E32, F32, G32,na.rm=T),
         Total.HighwaysTot.Exp = sum(E44, F44, G44, E45, F45, G45,na.rm=T),
         ParkingTotal.Expend = sum(E60, F60, G60,na.rm=T),
         Hous..ComTotal.Exp = sum(E50, F50, G50,na.rm=T),
         LibrariesTotal.Expend = sum(E52, F52, G52,na.rm=T),
         Parks..RecTotal.Exp = sum(E61, F61, G61,na.rm=T),
         SewerageTotal.Expend = sum(E80, F80, G80,na.rm=T),
         Total.UtilTotal.Exp = sum(E91, F91, G91, I91, E92, F92, G92, I92, E93, F93, G93, I93, E94, F94, G94, I94,na.rm=T),
         Public.WelfTotal.Exp = sum(J67, J68, 
                                    # E73, 
                                    E74, E75, E77, F77, G77, E79, F79, G79,na.rm=T),
         Total.Interest.on.Debt = sum(I89,na.rm=T),
         Fin.AdminTotal.Exp = sum(E23, F23, G23,na.rm=T),
         
         # debt categories
         Total.Debt.Outstanding = sum(`44T`, `49U`, `64V`,na.rm=T),
         LTD.OutGeneral = NA, # not sure; not in methodology doc
         Total.LTD.OutFFC = NA, # not sure; not in methodology doc
         Total.LTD.OutNG = NA, # not sure; not in methodology doc
         Total.LTD.Outstanding = sum(`44T`, `49U`,na.rm=T),
         
         # rev categories:
         General.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                               # A56,
                               A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                               # U21,
                               U30, U40, U41, U50, U95, U99,na.rm=T),
         General.RevOwn.Source = sum(A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                                     # A56, missing b/c only at state-fed level
                                     A59, A60, A61, A80, A81, A87, A89, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, U01, U11, U20, 
                                     # U21, missing b/c only at state-fed level
                                     U30, U40, U41, U50, U95, U99,na.rm=T),
         # Property.Tax = (T01,na.rm=T),
         Total.Taxes = sum(T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99,na.rm=T),
         Total.Select.Sales.Tax = sum(T10, T11, T12, T13, T14, T15, T16, T19,na.rm=T),
         Total.Gen.Sales.Tax = (T09),
         Total.State.IG.Revenue = sum(C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94,na.rm=T),
         Total.IG.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94,na.rm=T),
         
         Total.RevOwn.Sources = Total.Revenue - Total.IG.Revenue,
         
         Tot.Chgs.and.Misc.Rev = sum(A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                                     # A56, 
                                     A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                                     # U21, 
                                     U30, U40, U41, U50, U95, U99,na.rm=T),
         Total.Utility.Revenue = sum(A91, A92, A93, A94)
  )

## change names using list from here: https://www.census.gov/govs/www/stateloctechdoc.html#codedef
# findat_codes <- read_csv("finances_categories.csv") # not necessarily good for all years
# getting from tech doc PDF instead:
doc13 <- pdftools::pdf_text("2013_Individual_Unit_file/2013 S&L Public Use Files Technical Documentation.pdf")
doc13 <- doc13[6:14]
doc13 <- unlist(str_split(doc13,pattern = "\n"))
doc13 <- doc13[doc13!=""]
doc13 <- doc13[grep("Item Code\\W",doc13):length(doc13)]
doc13 <- data.frame(item_code = doc13,stringsAsFactors = F) %>% as_tibble()
doc13 <- doc13[-1,]
doc13$item_code <- str_trim(doc13$item_code)
doc13$var_name <- str_replace(doc13$item_code,"(\\w{3})\\W+(\\w.*)","\\2")
doc13$item_code <- str_replace(doc13$item_code,"(\\w{3})\\W+(\\w.*)","\\1")
doc13 <- doc13 %>%
  mutate(var_name = str_replace_all(var_name,c(" " = ".", "-" = "", "&" = "", "," = "","/" = "","\\.\\."=".")))

names(findat_2013_wide)[which(names(findat_2013_wide) %in% doc13$item_code)] <- doc13$var_name[match(names(findat_2013_wide)[which(names(findat_2013_wide) %in% doc13$item_code)], doc13$item_code)]
# eliminate a duplicate problem:
names(findat_2013_wide)[grep("State.Veterans'.AssistanceCurrent.Op",names(findat_2013_wide))[2]] <- "State.Veterans'.AssistanceCurrent.Op2"
save(findat_2013_wide, file="IndFin_2013.RData")
# load("IndFin_2013.RData")

## --------- ##
#### 2014: ####
## --------- ##
findat_2014 <- read_fwf("2014-individual-unit-file/2014FinEstDAT_10162019modp_pu.txt",
                        col_positions = fwf_widths(widths = c(14,3,12,4,1),
                                                   col_names = c("govid_14","item_code","amount","YearData","imputation"))
)
cwalk_2014 <- read_fwf("2014-individual-unit-file/Fin_GID_2014.txt",
                       col_positions = fwf_widths(widths = c(14,64,35,2,3,5,9,2,7,2,2,2,4,2),
                                                  col_names = c("govid_14","Name","CountyName",
                                                                "fips_state","fips_county","fips_place",
                                                                "population","pop_year","enrollment","enrollment_year",
                                                                "function_code","school_lvl_code","FYEndDate","SurvYear")))
findat_2014 <- left_join(findat_2014,cwalk_2014,by="govid_14")
findat_2014 <- findat_2014 %>% select(-imputation)
findat_2014_wide <- spread(findat_2014, key=item_code, value=amount)
# findat_2014_wide <- findat_2014_wide %>%
#   filter(imputation == "R") # eliminate imputed variables



## check that this is right number of governments: 
# length(unique(findat_2014$govid_14[findat_2014$imputation=="R"]))


## Aggregating data into broad categories using "publication aggregates" methodology file downloaded here: https://www.census.gov/data/datasets/2014/econ/local/public-use-datasets.html
findat_2014_wide <- findat_2014_wide %>%
  rowwise() %>%
  mutate(Total.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                             # A56, # Charges.Natural.Resources.Forestry missing b/c only at state-fed level
                             A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                             # U21, # missing b/c only at state-fed level
                             U30, U40, U41, U50, U95, U99, A90, A91, A92, A93, A94, X01, X02, X05, X08, Y01, Y02, Y04, Y11, Y12, Y51, Y52,na.rm=T),
         
         Total.Expenditure = sum(E01, E03, E04, E05, E12, E16, E18, E21, E22, E23, E24, E25,
                                 # E26, # Current.Operations.Legislative.Services missing b/c only at state-fed level
                                 E29, E31, E32, E36, E44, E45, E50, E52, 
                                 # E55, # Current.Operations.Natural.Resources.Fish.and.Game missing b/c only at state-fed level
                                 # E56, # Current.Operations.Natural.Resources.Forestry missing b/c only at state-fed level
                                 E59, E60, E61, E62, E66, 
                                 # E73, # missing b/c only at state-fed level
                                 E74, E75, E77, E79, E80, E81, E85, E87, E89, E90, E91, E92, E93, E94, I89, I91, I92, I93, I94, J19, L67, J68, J85, X11, X12, Y05, Y06, Y14, Y53, F01, F03, F04, F05, F12, F16, F18, F21, F22, F23, F24, F25, 
                                 # F26, # Construction.Legislative missing b/c only at state-fed level
                                 F29, F31, F32, F36, F44, F45, F50, F52,
                                 # F55, # Construction.Fish.and.Game missing b/c only at state-fed level
                                 # F56, # Construction.Forestry missing b/c only at state-fed level
                                 F59, F60, F61, F62, F66, F77, F79, F80, F81, F85, F87, F89, F90, F91, F92, F93, F94, G01, G03, G04, G05, G12, G16, G18, G21, G22, G23, G24, G25, 
                                 # G26, # Other.Capital.Outlay.Legislative missing b/c only at state-fed level
                                 G29, G31, G32, G36, G44, G45, G50, G52, 
                                 # G55, # Other.Capital.Outlay.Fish.and.Game missing b/c only at state-fed level
                                 # G56, # Other.Capital.Outlay.Forestry missing b/c only at state-fed level
                                 G59, G60, G61, G62, G66, G77, G79, G80, G81, G85, G87, G89, G90, G91, G92, G93, G94, J67, L01, L04, L05, L12, L18, L23, L25, L29, L32, L36, L44, L50, L52, L59, L60, L61, L62, L66, L67, L79, L80, L81, L87, L89, L91, L92, L93, L94, M01, M04, M05, M12, M18, M21, M23, M24, M25, M29, M30, M32, M36, M44, M50, M52, 
                                 # M55, # Intergovernmental.to.Local.NEC.Fish.and.Game missing b/c only at state-fed level
                                 # M56, # Intergovernmental.to.Local.NEC.Forestry missing b/c only at state-fed level
                                 M59, M60, M61, M62, M66, M67, M68, M79, M80, M81, M87, M89, M91, M92, M93, M94, Q12, Q18, S67, 
                                 # S74, # missing b/c only at state-fed level
                                 S89,na.rm=T),
         
         # exp categories:
         Direct.Expenditure = sum(E01, E03, E04, E05, E12, E16, E18, E21, E22, E23, E24, E25,
                                  # E26, 
                                  E29, E31, E32, E36, E44, E45, E50, E52, 
                                  # E55, E56,
                                  E59, E60, E61, E62, E66, 
                                  # E73,
                                  E74, E75, E77, E79, E80, E81, E85, E87, E89, E90, E91, E92, E93, E94, F01, F03, F04, F05, F12, F16, F18, F21, F22, F23, F24, F25,
                                  # F26, 
                                  F29, F31, F32, F36, F44, F45, F50, F52,
                                  # F55, F56, 
                                  F59, F60, F61, F62, F66, F77, F79, F80, F81, F85, F87, F89, F90, F91, F92, F93, F94, G01, G03, G04, G05, G12, G16, G18, G21, G22, G23, G24, G25, 
                                  # G26, 
                                  G29, G31, G32, G36, G44, G45, G50, G52, 
                                  # G55, G56,
                                  G59, G60, G61, G62, G66, G77, G79, G80, G81, G85, G87, G89, G90, G91, G92, G93, G94, X11, X12, Y05, Y06, Y14, Y53, J19, J67, J68, J85, I89, I91, I92, I93, I94,na.rm=T),
         
         Total.HospitalTotal.Exp = sum(E36, F36, G36,na.rm=T),
         CorrectTotal.Exp = sum(E04, F04, G04, E05, F05, G05,na.rm=T),
         Natural.ResTotal.Exp = sum(
           # E55, F55, G55, E56, F56, G56,
           E59, F59, G59,na.rm=T),
         Total.EducTotal.Exp = sum(E12, F12, G12, E16, F16, G16, E18, F18, G18, J19, E21, F21, G21,na.rm=T),
         General.NECTotal.Exp = sum(E89, F89, G89,na.rm=T), # called "other and unallocable" in methodology doc
         Fire.ProtTotal.Expend = sum(E24, F24, G24,na.rm=T),
         Police.ProtTotal.Exp = sum(E62, F62, G62,na.rm=T),
         HealthTotal.Expend = sum(E32, F32, G32,na.rm=T),
         Total.HighwaysTot.Exp = sum(E44, F44, G44, E45, F45, G45,na.rm=T),
         ParkingTotal.Expend = sum(E60, F60, G60,na.rm=T),
         Hous..ComTotal.Exp = sum(E50, F50, G50,na.rm=T),
         LibrariesTotal.Expend = sum(E52, F52, G52,na.rm=T),
         Parks..RecTotal.Exp = sum(E61, F61, G61,na.rm=T),
         SewerageTotal.Expend = sum(E80, F80, G80,na.rm=T),
         Total.UtilTotal.Exp = sum(E91, F91, G91, I91, E92, F92, G92, I92, E93, F93, G93, I93, E94, F94, G94, I94,na.rm=T),
         Public.WelfTotal.Exp = sum(J67, J68, 
                                    # E73, 
                                    E74, E75, E77, F77, G77, E79, F79, G79,na.rm=T),
         Total.Interest.on.Debt = sum(I89,na.rm=T),
         Fin.AdminTotal.Exp = sum(E23, F23, G23,na.rm=T),
         
         # debt categories
         Total.Debt.Outstanding = sum(`44T`, `49U`, `64V`,na.rm=T),
         LTD.OutGeneral = NA, # not sure; not in methodology doc
         Total.LTD.OutFFC = NA, # not sure; not in methodology doc
         Total.LTD.OutNG = NA, # not sure; not in methodology doc
         Total.LTD.Outstanding = sum(`44T`, `49U`,na.rm=T),
         
         # rev categories:
         General.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                               # A56,
                               A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                               # U21,
                               U30, U40, U41, U50, U95, U99,na.rm=T),
         General.RevOwn.Source = sum(A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                                     # A56, missing b/c only at state-fed level
                                     A59, A60, A61, A80, A81, A87, A89, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, U01, U11, U20, 
                                     # U21, missing b/c only at state-fed level
                                     U30, U40, U41, U50, U95, U99,na.rm=T),
         # Property.Tax = (T01,na.rm=T),
         Total.Taxes = sum(T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99,na.rm=T),
         Total.Select.Sales.Tax = sum(T10, T11, T12, T13, T14, T15, T16, T19,na.rm=T),
         Total.Gen.Sales.Tax = (T09),
         Total.State.IG.Revenue = sum(C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94,na.rm=T),
         Total.IG.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94,na.rm=T),
         
         Total.RevOwn.Sources = Total.Revenue - Total.IG.Revenue,
         
         Tot.Chgs.and.Misc.Rev = sum(A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                                     # A56, 
                                     A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                                     # U21, 
                                     U30, U40, U41, U50, U95, U99,na.rm=T),
         Total.Utility.Revenue = sum(A91, A92, A93, A94)
  )


## change names using tech doc PDF:
doc14 <- pdftools::pdf_text("2014-individual-unit-file/2014 S&L Public Use Files Technical Documentation.pdf")
doc14 <- doc14[6:14]
doc14 <- unlist(str_split(doc14,pattern = "\n"))
doc14 <- doc14[doc14!=""]
doc14 <- doc14[grep("Item Code\\W",doc14):length(doc14)]
doc14 <- data.frame(item_code = doc14,stringsAsFactors = F) %>% as_tibble()
doc14 <- doc14[-1,]
doc14$item_code <- str_trim(doc14$item_code)
doc14$var_name <- str_replace(doc14$item_code,"(\\w{3})\\W+(\\w.*)","\\2")
doc14$item_code <- str_replace(doc14$item_code,"(\\w{3})\\W+(\\w.*)","\\1")
doc14 <- doc14 %>%
  mutate(var_name = str_replace_all(var_name,c(" " = ".", "-" = "", "&" = "", "," = "","/" = "","\\.\\."=".")))

names(findat_2014_wide)[which(names(findat_2014_wide) %in% doc14$item_code)] <- doc14$var_name[match(names(findat_2014_wide)[which(names(findat_2014_wide) %in% doc14$item_code)], doc14$item_code)]
names(findat_2014_wide)[grep("State.Veterans'.AssistanceCurrent.Op",names(findat_2014_wide))[2]] <- "State.Veterans'.AssistanceCurrent.Op2"

save(findat_2014_wide, file="IndFin_2014.RData")
# load("IndFin_2014.RData")


## --------- ##
#### 2015: ####
## --------- ##
findat_2015 <- read_fwf("2015-individual-unit-file/2015FinEstDAT_10162019modp_pu.txt",
                        col_positions = fwf_widths(widths = c(14,3,12,4,1),
                                                   col_names = c("govid_14","item_code","amount","YearData","imputation"))
)
cwalk_2015 <- read_fwf("2015-individual-unit-file/Fin_GID_2015.txt",
                       col_positions = fwf_widths(widths = c(14,64,35,2,3,5,9,2,7,2,2,2,4,2),
                                                  col_names = c("govid_14","Name","CountyName",
                                                                "fips_state","fips_county","fips_place",
                                                                "population","pop_year","enrollment","enrollment_year",
                                                                "function_code","school_lvl_code","FYEndDate","SurvYear")))
findat_2015 <- left_join(findat_2015,cwalk_2015,by="govid_14")
findat_2015 <- findat_2015 %>% select(-imputation) # ignore imputation
findat_2015_wide <- spread(findat_2015, key=item_code, value=amount)
# findat_2015_wide <- findat_2015_wide %>%
#   filter(imputation == "R") # eliminate imputed variables
## check that this is right number of governments: 
# length(unique(findat_2015$govid_14[findat_2015$imputation=="R"]))


## Aggregating data into broad categories using "publication aggregates" methodology file downloaded for 2014
findat_2015_wide <- findat_2015_wide %>%
  rowwise() %>%
  mutate(Total.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                             # A56, # Charges.Natural.Resources.Forestry missing b/c only at state-fed level
                             A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                             # U21, # missing b/c only at state-fed level
                             U30, U40, U41, U50, U95, U99, A90, A91, A92, A93, A94, X01, X02, X05, X08, Y01, Y02, Y04, Y11, Y12, Y51, Y52,na.rm=T),
         
         Total.Expenditure = sum(E01, E03, E04, E05, E12, E16, E18, E21, E22, E23, E24, E25,
                                 # E26, # Current.Operations.Legislative.Services missing b/c only at state-fed level
                                 E29, E31, E32, E36, E44, E45, E50, E52, 
                                 # E55, # Current.Operations.Natural.Resources.Fish.and.Game missing b/c only at state-fed level
                                 # E56, # Current.Operations.Natural.Resources.Forestry missing b/c only at state-fed level
                                 E59, E60, E61, E62, E66, 
                                 # E73, # missing b/c only at state-fed level
                                 E74, E75, E77, E79, E80, E81, E85, E87, E89, E90, E91, E92, E93, E94, I89, I91, I92, I93, I94, J19, L67, J68, J85, X11, X12, Y05, Y06, Y14, Y53, F01, F03, F04, F05, F12, F16, F18, F21, F22, F23, F24, F25, 
                                 # F26, # Construction.Legislative missing b/c only at state-fed level
                                 F29, F31, F32, F36, F44, F45, F50, F52,
                                 # F55, # Construction.Fish.and.Game missing b/c only at state-fed level
                                 # F56, # Construction.Forestry missing b/c only at state-fed level
                                 F59, F60, F61, F62, F66, F77, F79, F80, F81, F85, F87, F89, F90, F91, F92, F93, F94, G01, G03, G04, G05, G12, G16, G18, G21, G22, G23, G24, G25, 
                                 # G26, # Other.Capital.Outlay.Legislative missing b/c only at state-fed level
                                 G29, G31, G32, G36, G44, G45, G50, G52, 
                                 # G55, # Other.Capital.Outlay.Fish.and.Game missing b/c only at state-fed level
                                 # G56, # Other.Capital.Outlay.Forestry missing b/c only at state-fed level
                                 G59, G60, G61, G62, G66, G77, G79, G80, G81, G85, G87, G89, G90, G91, G92, G93, G94, J67, L01, L04, L05, L12, L18, L23, L25, L29, L32, L36, L44, L50, L52, L59, L60, L61, L62, L66, L67, L79, L80, L81, L87, L89, L91, L92, L93, L94, M01, M04, M05, M12, M18, M21, M23, M24, M25, M29, M30, M32, M36, M44, M50, M52, 
                                 # M55, # Intergovernmental.to.Local.NEC.Fish.and.Game missing b/c only at state-fed level
                                 # M56, # Intergovernmental.to.Local.NEC.Forestry missing b/c only at state-fed level
                                 M59, M60, M61, M62, M66, M67, M68, M79, M80, M81, M87, M89, M91, M92, M93, M94, Q12, Q18, S67, 
                                 # S74, # missing b/c only at state-fed level
                                 S89,na.rm=T),
         
         # exp categories:
         Direct.Expenditure = sum(E01, E03, E04, E05, E12, E16, E18, E21, E22, E23, E24, E25,
                                  # E26, 
                                  E29, E31, E32, E36, E44, E45, E50, E52, 
                                  # E55, E56,
                                  E59, E60, E61, E62, E66, 
                                  # E73,
                                  E74, E75, E77, E79, E80, E81, E85, E87, E89, E90, E91, E92, E93, E94, F01, F03, F04, F05, F12, F16, F18, F21, F22, F23, F24, F25,
                                  # F26, 
                                  F29, F31, F32, F36, F44, F45, F50, F52,
                                  # F55, F56, 
                                  F59, F60, F61, F62, F66, F77, F79, F80, F81, F85, F87, F89, F90, F91, F92, F93, F94, G01, G03, G04, G05, G12, G16, G18, G21, G22, G23, G24, G25, 
                                  # G26, 
                                  G29, G31, G32, G36, G44, G45, G50, G52, 
                                  # G55, G56,
                                  G59, G60, G61, G62, G66, G77, G79, G80, G81, G85, G87, G89, G90, G91, G92, G93, G94, X11, X12, Y05, Y06, Y14, Y53, J19, J67, J68, J85, I89, I91, I92, I93, I94,na.rm=T),
         
         Total.HospitalTotal.Exp = sum(E36, F36, G36,na.rm=T),
         CorrectTotal.Exp = sum(E04, F04, G04, E05, F05, G05,na.rm=T),
         Natural.ResTotal.Exp = sum(
           # E55, F55, G55, E56, F56, G56,
           E59, F59, G59,na.rm=T),
         Total.EducTotal.Exp = sum(E12, F12, G12, E16, F16, G16, E18, F18, G18, J19, E21, F21, G21,na.rm=T),
         General.NECTotal.Exp = sum(E89, F89, G89,na.rm=T), # called "other and unallocable" in methodology doc
         Fire.ProtTotal.Expend = sum(E24, F24, G24,na.rm=T),
         Police.ProtTotal.Exp = sum(E62, F62, G62,na.rm=T),
         HealthTotal.Expend = sum(E32, F32, G32,na.rm=T),
         Total.HighwaysTot.Exp = sum(E44, F44, G44, E45, F45, G45,na.rm=T),
         ParkingTotal.Expend = sum(E60, F60, G60,na.rm=T),
         Hous..ComTotal.Exp = sum(E50, F50, G50,na.rm=T),
         LibrariesTotal.Expend = sum(E52, F52, G52,na.rm=T),
         Parks..RecTotal.Exp = sum(E61, F61, G61,na.rm=T),
         SewerageTotal.Expend = sum(E80, F80, G80,na.rm=T),
         Total.UtilTotal.Exp = sum(E91, F91, G91, I91, E92, F92, G92, I92, E93, F93, G93, I93, E94, F94, G94, I94,na.rm=T),
         Public.WelfTotal.Exp = sum(J67, J68, 
                                    # E73, 
                                    E74, E75, E77, F77, G77, E79, F79, G79,na.rm=T),
         Total.Interest.on.Debt = sum(I89,na.rm=T),
         Fin.AdminTotal.Exp = sum(E23, F23, G23,na.rm=T),
         
         # debt categories
         Total.Debt.Outstanding = sum(`44T`, `49U`, `64V`,na.rm=T),
         LTD.OutGeneral = NA, # not sure; not in methodology doc
         Total.LTD.OutFFC = NA, # not sure; not in methodology doc
         Total.LTD.OutNG = NA, # not sure; not in methodology doc
         Total.LTD.Outstanding = sum(`44T`, `49U`,na.rm=T),
         
         # rev categories:
         General.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                               # A56,
                               A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                               # U21,
                               U30, U40, U41, U50, U95, U99,na.rm=T),
         General.RevOwn.Source = sum(A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                                     # A56, missing b/c only at state-fed level
                                     A59, A60, A61, A80, A81, A87, A89, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, U01, U11, U20, 
                                     # U21, missing b/c only at state-fed level
                                     U30, U40, U41, U50, U95, U99,na.rm=T),
         # Property.Tax = (T01,na.rm=T),
         Total.Taxes = sum(T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99,na.rm=T),
         Total.Select.Sales.Tax = sum(T10, T11, T12, T13, T14, T15, T16, T19,na.rm=T),
         Total.Gen.Sales.Tax = (T09),
         Total.State.IG.Revenue = sum(C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94,na.rm=T),
         Total.IG.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94,na.rm=T),
         
         Total.RevOwn.Sources = Total.Revenue - Total.IG.Revenue,
         
         Tot.Chgs.and.Misc.Rev = sum(A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                                     # A56, 
                                     A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                                     # U21, 
                                     U30, U40, U41, U50, U95, U99,na.rm=T),
         Total.Utility.Revenue = sum(A91, A92, A93, A94)
  )

## change names using tech doc PDF:
doc15 <- pdftools::pdf_text("2015-individual-unit-file/2015 S&L Public Use Files Technical Documentation.pdf")
doc15 <- doc15[6:14]
doc15 <- unlist(str_split(doc15,pattern = "\n"))
doc15 <- doc15[doc15!=""]
doc15 <- doc15[grep("Item Code\\W",doc15):length(doc15)]
doc15 <- data.frame(item_code = doc15,stringsAsFactors = F) %>% as_tibble()
doc15 <- doc15[-1,]
doc15$item_code <- str_trim(doc15$item_code)
doc15$var_name <- str_replace(doc15$item_code,"(\\w{3})\\W+(\\w.*)","\\2")
doc15$item_code <- str_replace(doc15$item_code,"(\\w{3})\\W+(\\w.*)","\\1")
doc15 <- doc15 %>%
  mutate(var_name = str_replace_all(var_name,c(" " = ".", "-" = "", "&" = "", "," = "","/" = "","\\.\\."=".")))

names(findat_2015_wide)[which(names(findat_2015_wide) %in% doc15$item_code)] <- doc15$var_name[match(names(findat_2015_wide)[which(names(findat_2015_wide) %in% doc15$item_code)], doc15$item_code)]
names(findat_2015_wide)[grep("State.Veterans'.AssistanceCurrent.Op",names(findat_2015_wide))[2]] <- "State.Veterans'.AssistanceCurrent.Op2"


save(findat_2015_wide, file="IndFin_2015.RData")
# load("IndFin_2015.RData")


## --------- ##
#### 2016: ####
## --------- ##
findat_2016 <- read_fwf("2016_Individual_Unit_file/2016FinEstDAT_10162019modp_pu.txt",
                        col_positions = fwf_widths(widths = c(14,3,12,4,1),
                                                   col_names = c("govid_14","item_code","amount","YearData","imputation"))
)
cwalk_2016 <- read_fwf("2016_Individual_Unit_file/Fin_GID_2016.txt",
                       col_positions = fwf_widths(widths = c(14,64,35,2,3,5,9,2,7,2,2,2,4,2),
                                                  col_names = c("govid_14","Name","CountyName",
                                                                "fips_state","fips_county","fips_place",
                                                                "population","pop_year","enrollment","enrollment_year",
                                                                "function_code","school_lvl_code","FYEndDate","SurvYear")))
findat_2016 <- left_join(findat_2016,cwalk_2016,by="govid_14")
findat_2016 <- findat_2016 %>% select(-imputation) # ignore imputation

findat_2016_wide <- spread(findat_2016, key=item_code, value=amount)
# findat_2016_wide <- findat_2016_wide %>%
  # filter(imputation == "R") # eliminate imputed variables
## check that this is right number of governments: 
# length(unique(findat_2016$govid_14[findat_2016$imputation=="R"]))


## Aggregating data into broad categories using "publication aggregates" methodology file downloaded for 2014
findat_2016_wide <- findat_2016_wide %>%
  rowwise() %>%
  mutate(Total.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                             # A56, # Charges.Natural.Resources.Forestry missing b/c only at state-fed level
                             A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                             # U21, # missing b/c only at state-fed level
                             U30, U40, U41, U50, U95, U99, A90, A91, A92, A93, A94, X01, X02, X05, X08, Y01, Y02, 
                             # Y04, # no longer in as of 2016
                             Y11, Y12, Y51, Y52,na.rm=T),
         
         Total.Expenditure = sum(E01, E03, E04, E05, E12, E16, E18, E21, E22, E23, E24, E25,
                                 # E26, # Current.Operations.Legislative.Services missing b/c only at state-fed level
                                 E29, E31, E32, E36, E44, E45, E50, E52, 
                                 # E55, # Current.Operations.Natural.Resources.Fish.and.Game missing b/c only at state-fed level
                                 # E56, # Current.Operations.Natural.Resources.Forestry missing b/c only at state-fed level
                                 E59, E60, E61, E62, E66, 
                                 # E73, # missing b/c only at state-fed level
                                 E74, E75, E77, E79, E80, E81, E85, E87, E89, E90, E91, E92, E93, E94, I89, I91, I92, I93, I94, J19, L67, J68, J85, X11, X12, Y05, Y06, Y14, Y53, F01, F03, F04, F05, F12, F16, F18, F21, F22, F23, F24, F25, 
                                 # F26, # Construction.Legislative missing b/c only at state-fed level
                                 F29, F31, F32, F36, F44, F45, F50, F52,
                                 # F55, # Construction.Fish.and.Game missing b/c only at state-fed level
                                 # F56, # Construction.Forestry missing b/c only at state-fed level
                                 F59, F60, F61, F62, F66, F77, F79, F80, F81, F85, F87, F89, F90, F91, F92, F93, F94, G01, G03, G04, G05, G12, G16, G18, G21, G22, G23, G24, G25, 
                                 # G26, # Other.Capital.Outlay.Legislative missing b/c only at state-fed level
                                 G29, G31, G32, G36, G44, G45, G50, G52, 
                                 # G55, # Other.Capital.Outlay.Fish.and.Game missing b/c only at state-fed level
                                 # G56, # Other.Capital.Outlay.Forestry missing b/c only at state-fed level
                                 G59, G60, G61, G62, G66, G77, G79, G80, G81, G85, G87, G89, G90, G91, G92, G93, G94, J67, L01, L04, L05, L12, L18, L23, L25, L29, L32, L36, L44, L50, L52, L59, L60, L61, L62, L66, L67, L79, L80, L81, L87, L89, L91, L92, L93, L94, M01, M04, M05, M12, M18, M21, M23, M24, M25, M29, M30, M32, M36, M44, M50, M52, 
                                 # M55, # Intergovernmental.to.Local.NEC.Fish.and.Game missing b/c only at state-fed level
                                 # M56, # Intergovernmental.to.Local.NEC.Forestry missing b/c only at state-fed level
                                 M59, M60, M61, M62, M66, M67, M68, M79, M80, M81, M87, M89, M91, M92, M93, M94, Q12, Q18, S67, 
                                 # S74, # missing b/c only at state-fed level
                                 S89,na.rm=T),
         
         # exp categories:
         Direct.Expenditure = sum(E01, E03, E04, E05, E12, E16, E18, E21, E22, E23, E24, E25,
                                  # E26, 
                                  E29, E31, E32, E36, E44, E45, E50, E52, 
                                  # E55, E56,
                                  E59, E60, E61, E62, E66, 
                                  # E73,
                                  E74, E75, E77, E79, E80, E81, E85, E87, E89, E90, E91, E92, E93, E94, F01, F03, F04, F05, F12, F16, F18, F21, F22, F23, F24, F25,
                                  # F26, 
                                  F29, F31, F32, F36, F44, F45, F50, F52,
                                  # F55, F56, 
                                  F59, F60, F61, F62, F66, F77, F79, F80, F81, F85, F87, F89, F90, F91, F92, F93, F94, G01, G03, G04, G05, G12, G16, G18, G21, G22, G23, G24, G25, 
                                  # G26, 
                                  G29, G31, G32, G36, G44, G45, G50, G52, 
                                  # G55, G56,
                                  G59, G60, G61, G62, G66, G77, G79, G80, G81, G85, G87, G89, G90, G91, G92, G93, G94, X11, X12, Y05, Y06, Y14, Y53, J19, J67, J68, J85, I89, I91, I92, I93, I94,na.rm=T),
         
         Total.HospitalTotal.Exp = sum(E36, F36, G36,na.rm=T),
         CorrectTotal.Exp = sum(E04, F04, G04, E05, F05, G05,na.rm=T),
         Natural.ResTotal.Exp = sum(
           # E55, F55, G55, E56, F56, G56,
           E59, F59, G59,na.rm=T),
         Total.EducTotal.Exp = sum(E12, F12, G12, E16, F16, G16, E18, F18, G18, J19, E21, F21, G21,na.rm=T),
         General.NECTotal.Exp = sum(E89, F89, G89,na.rm=T), # called "other and unallocable" in methodology doc
         Fire.ProtTotal.Expend = sum(E24, F24, G24,na.rm=T),
         Police.ProtTotal.Exp = sum(E62, F62, G62,na.rm=T),
         HealthTotal.Expend = sum(E32, F32, G32,na.rm=T),
         Total.HighwaysTot.Exp = sum(E44, F44, G44, E45, F45, G45,na.rm=T),
         ParkingTotal.Expend = sum(E60, F60, G60,na.rm=T),
         Hous..ComTotal.Exp = sum(E50, F50, G50,na.rm=T),
         LibrariesTotal.Expend = sum(E52, F52, G52,na.rm=T),
         Parks..RecTotal.Exp = sum(E61, F61, G61,na.rm=T),
         SewerageTotal.Expend = sum(E80, F80, G80,na.rm=T),
         Total.UtilTotal.Exp = sum(E91, F91, G91, I91, E92, F92, G92, I92, E93, F93, G93, I93, E94, F94, G94, I94,na.rm=T),
         Public.WelfTotal.Exp = sum(J67, J68, 
                                    # E73, 
                                    E74, E75, E77, F77, G77, E79, F79, G79,na.rm=T),
         Total.Interest.on.Debt = sum(I89,na.rm=T),
         Fin.AdminTotal.Exp = sum(E23, F23, G23,na.rm=T),
         
         # debt categories
         Total.Debt.Outstanding = sum(`44T`, `49U`, `64V`,na.rm=T),
         LTD.OutGeneral = NA, # not sure; not in methodology doc
         Total.LTD.OutFFC = NA, # not sure; not in methodology doc
         Total.LTD.OutNG = NA, # not sure; not in methodology doc
         Total.LTD.Outstanding = sum(`44T`, `49U`,na.rm=T),
         
         # rev categories:
         General.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                               # A56,
                               A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                               # U21,
                               U30, U40, U41, U50, U95, U99,na.rm=T),
         General.RevOwn.Source = sum(A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                                     # A56, missing b/c only at state-fed level
                                     A59, A60, A61, A80, A81, A87, A89, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, U01, U11, U20, 
                                     # U21, missing b/c only at state-fed level
                                     U30, U40, U41, U50, U95, U99,na.rm=T),
         # Property.Tax = (T01,na.rm=T),
         Total.Taxes = sum(T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99,na.rm=T),
         Total.Select.Sales.Tax = sum(T10, T11, T12, T13, T14, T15, T16, T19,na.rm=T),
         Total.Gen.Sales.Tax = (T09),
         Total.State.IG.Revenue = sum(C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94,na.rm=T),
         Total.IG.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94,na.rm=T),
         
         Total.RevOwn.Sources = Total.Revenue - Total.IG.Revenue,
         
         Tot.Chgs.and.Misc.Rev = sum(A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                                     # A56, 
                                     A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                                     # U21, 
                                     U30, U40, U41, U50, U95, U99,na.rm=T),
         Total.Utility.Revenue = sum(A91, A92, A93, A94)
  )

## change names using tech doc PDF:
doc16 <- pdftools::pdf_text("2016_Individual_Unit_file/2016 S&L Public Use Files Technical Documentation.pdf")
doc16 <- doc16[6:14]
doc16 <- unlist(str_split(doc16,pattern = "\n"))
doc16 <- doc16[doc16!=""]
doc16 <- doc16[grep("Item Code\\W",doc16):length(doc16)]
doc16 <- data.frame(item_code = doc16,stringsAsFactors = F) %>% as_tibble()
doc16 <- doc16[-1,]
doc16$item_code <- str_trim(doc16$item_code)
doc16$var_name <- str_replace(doc16$item_code,"(\\w{3})\\W+(\\w.*)","\\2")
doc16$item_code <- str_replace(doc16$item_code,"(\\w{3})\\W+(\\w.*)","\\1")
doc16 <- doc16 %>%
  mutate(var_name = str_replace_all(var_name,c(" " = ".", "-" = "", "&" = "", "," = "","/" = "","\\.\\."=".")))

names(findat_2016_wide)[which(names(findat_2016_wide) %in% doc16$item_code)] <- doc16$var_name[match(names(findat_2016_wide)[which(names(findat_2016_wide) %in% doc16$item_code)], doc16$item_code)]
names(findat_2016_wide)[grep("State.Veterans'.AssistanceCurrent.Op",names(findat_2016_wide))[2]] <- "State.Veterans'.AssistanceCurrent.Op2"


save(findat_2016_wide, file="IndFin_2016.RData")
# load("IndFin_2016.RData")


## --------- ##
#### 2017: ####
## --------- ##
findat_2017 <- read_fwf("2017_Individual_Unit_file/2017FinEstDAT_02202020modp_pu.txt",
                        col_positions = fwf_widths(widths = c(14,3,12,4,1),
                                                   col_names = c("govid_14","item_code","amount","YearData","imputation"))
)
cwalk_2017 <- read_fwf("2017_Individual_Unit_file/Fin_GID_2017.txt",
                       col_positions = fwf_widths(widths = c(14,64,35,2,3,5,9,2,7,2,2,2,4,2),
                                                  col_names = c("govid_14","Name","CountyName",
                                                                "fips_state","fips_county","fips_place",
                                                                "population","pop_year","enrollment","enrollment_year",
                                                                "function_code","school_lvl_code","FYEndDate","SurvYear")))
findat_2017 <- left_join(findat_2017,cwalk_2017,by="govid_14")
findat_2017 <- findat_2017 %>% 
  group_by(govid_14,item_code) %>%
  mutate(dupe = n()>1)
# findat_2017 %>% filter(dupe==1) %>% View()
findat_2017 <- findat_2017 %>%
  filter(!(is.na(imputation) & dupe==1)) # eliminate double entries in 3 counties for item code X08
findat_2017 <- findat_2017 %>% select(-imputation,-dupe) # ignore imputation

findat_2017_wide <- spread(findat_2017, key=item_code, value=amount)
# findat_2017_wide <- findat_2017_wide %>%
#   filter(imputation == "R") # eliminate imputed variables
## check that this is right number of governments: 
# length(unique(findat_2017$govid_14[findat_2017$imputation=="R"])) # missing one?


## Aggregating data into broad categories using "publication aggregates" methodology file downloaded for 2014
findat_2017_wide <- findat_2017_wide %>%
  rowwise() %>%
  mutate(Total.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                             # A56, # Charges.Natural.Resources.Forestry missing b/c only at state-fed level
                             A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                             # U21, # missing b/c only at state-fed level
                             U30, U40, U41, U50, U95, U99, A90, A91, A92, A93, A94, X01, X02, X05, X08, Y01, Y02, 
                             # Y04,  # no longer in as of 2016
                             Y11, Y12, Y51, Y52,na.rm=T),
         
         Total.Expenditure = sum(E01, E03, E04, E05, E12, E16, E18, E21, E22, E23, E24, E25,
                                 # E26, # Current.Operations.Legislative.Services missing b/c only at state-fed level
                                 E29, E31, E32, E36, E44, E45, E50, E52, 
                                 # E55, # Current.Operations.Natural.Resources.Fish.and.Game missing b/c only at state-fed level
                                 # E56, # Current.Operations.Natural.Resources.Forestry missing b/c only at state-fed level
                                 E59, E60, E61, E62, E66, 
                                 # E73, # missing b/c only at state-fed level
                                 E74, E75, E77, E79, E80, E81, E85, E87, E89, E90, E91, E92, E93, E94, I89, I91, I92, I93, I94, J19, L67, J68, J85, X11, X12, Y05, Y06, Y14, Y53, F01, F03, F04, F05, F12, F16, F18, F21, F22, F23, F24, F25, 
                                 # F26, # Construction.Legislative missing b/c only at state-fed level
                                 F29, F31, F32, F36, F44, F45, F50, F52,
                                 # F55, # Construction.Fish.and.Game missing b/c only at state-fed level
                                 # F56, # Construction.Forestry missing b/c only at state-fed level
                                 F59, F60, F61, F62, F66, F77, F79, F80, F81, F85, F87, F89, F90, F91, F92, F93, F94, G01, G03, G04, G05, G12, G16, G18, G21, G22, G23, G24, G25, 
                                 # G26, # Other.Capital.Outlay.Legislative missing b/c only at state-fed level
                                 G29, G31, G32, G36, G44, G45, G50, G52, 
                                 # G55, # Other.Capital.Outlay.Fish.and.Game missing b/c only at state-fed level
                                 # G56, # Other.Capital.Outlay.Forestry missing b/c only at state-fed level
                                 G59, G60, G61, G62, G66, G77, G79, G80, G81, G85, G87, G89, G90, G91, G92, G93, G94, J67, L01, L04, L05, L12, L18, L23, L25, L29, L32, L36, L44, L50, L52, L59, L60, L61, L62, L66, L67, L79, L80, L81, L87, L89, L91, L92, L93, L94, M01, M04, M05, M12, M18, M21, M23, M24, M25, M29, M30, M32, M36, M44, M50, M52, 
                                 # M55, # Intergovernmental.to.Local.NEC.Fish.and.Game missing b/c only at state-fed level
                                 # M56, # Intergovernmental.to.Local.NEC.Forestry missing b/c only at state-fed level
                                 M59, M60, M61, M62, M66, M67, M68, M79, M80, M81, M87, M89, M91, M92, M93, M94, Q12, Q18, S67, 
                                 # S74, # missing b/c only at state-fed level
                                 S89,na.rm=T),
         
         # exp categories:
         Direct.Expenditure = sum(E01, E03, E04, E05, E12, E16, E18, E21, E22, E23, E24, E25,
                                  # E26, 
                                  E29, E31, E32, E36, E44, E45, E50, E52, 
                                  # E55, E56,
                                  E59, E60, E61, E62, E66, 
                                  # E73,
                                  E74, E75, E77, E79, E80, E81, E85, E87, E89, E90, E91, E92, E93, E94, F01, F03, F04, F05, F12, F16, F18, F21, F22, F23, F24, F25,
                                  # F26, 
                                  F29, F31, F32, F36, F44, F45, F50, F52,
                                  # F55, F56, 
                                  F59, F60, F61, F62, F66, F77, F79, F80, F81, F85, F87, F89, F90, F91, F92, F93, F94, G01, G03, G04, G05, G12, G16, G18, G21, G22, G23, G24, G25, 
                                  # G26, 
                                  G29, G31, G32, G36, G44, G45, G50, G52, 
                                  # G55, G56,
                                  G59, G60, G61, G62, G66, G77, G79, G80, G81, G85, G87, G89, G90, G91, G92, G93, G94, X11, X12, Y05, Y06, Y14, Y53, J19, J67, J68, J85, I89, I91, I92, I93, I94,na.rm=T),
         
         Total.HospitalTotal.Exp = sum(E36, F36, G36,na.rm=T),
         CorrectTotal.Exp = sum(E04, F04, G04, E05, F05, G05,na.rm=T),
         Natural.ResTotal.Exp = sum(
           # E55, F55, G55, E56, F56, G56,
           E59, F59, G59,na.rm=T),
         Total.EducTotal.Exp = sum(E12, F12, G12, E16, F16, G16, E18, F18, G18, J19, E21, F21, G21,na.rm=T),
         General.NECTotal.Exp = sum(E89, F89, G89,na.rm=T), # called "other and unallocable" in methodology doc
         Fire.ProtTotal.Expend = sum(E24, F24, G24,na.rm=T),
         Police.ProtTotal.Exp = sum(E62, F62, G62,na.rm=T),
         HealthTotal.Expend = sum(E32, F32, G32,na.rm=T),
         Total.HighwaysTot.Exp = sum(E44, F44, G44, E45, F45, G45,na.rm=T),
         ParkingTotal.Expend = sum(E60, F60, G60,na.rm=T),
         Hous..ComTotal.Exp = sum(E50, F50, G50,na.rm=T),
         LibrariesTotal.Expend = sum(E52, F52, G52,na.rm=T),
         Parks..RecTotal.Exp = sum(E61, F61, G61,na.rm=T),
         SewerageTotal.Expend = sum(E80, F80, G80,na.rm=T),
         Total.UtilTotal.Exp = sum(E91, F91, G91, I91, E92, F92, G92, I92, E93, F93, G93, I93, E94, F94, G94, I94,na.rm=T),
         Public.WelfTotal.Exp = sum(J67, J68, 
                                    # E73, 
                                    E74, E75, E77, F77, G77, E79, F79, G79,na.rm=T),
         Total.Interest.on.Debt = sum(I89,na.rm=T),
         Fin.AdminTotal.Exp = sum(E23, F23, G23,na.rm=T),
         
         # debt categories
         Total.Debt.Outstanding = sum(`44T`, `49U`, `64V`,na.rm=T),
         LTD.OutGeneral = NA, # not sure; not in methodology doc
         Total.LTD.OutFFC = NA, # not sure; not in methodology doc
         Total.LTD.OutNG = NA, # not sure; not in methodology doc
         Total.LTD.Outstanding = sum(`44T`, `49U`,na.rm=T),
         
         # rev categories:
         General.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                               # A56,
                               A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                               # U21,
                               U30, U40, U41, U50, U95, U99,na.rm=T),
         General.RevOwn.Source = sum(A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                                     # A56, missing b/c only at state-fed level
                                     A59, A60, A61, A80, A81, A87, A89, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, U01, U11, U20, 
                                     # U21, missing b/c only at state-fed level
                                     U30, U40, U41, U50, U95, U99,na.rm=T),
         # Property.Tax = (T01,na.rm=T),
         Total.Taxes = sum(T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99,na.rm=T),
         Total.Select.Sales.Tax = sum(T10, T11, T12, T13, T14, T15, T16, T19,na.rm=T),
         Total.Gen.Sales.Tax = (T09),
         Total.State.IG.Revenue = sum(C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94,na.rm=T),
         Total.IG.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94,na.rm=T),
         
         Total.RevOwn.Sources = Total.Revenue - Total.IG.Revenue,
         
         Tot.Chgs.and.Misc.Rev = sum(A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                                     # A56, 
                                     A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                                     # U21, 
                                     U30, U40, U41, U50, U95, U99,na.rm=T),
         Total.Utility.Revenue = sum(A91, A92, A93, A94)
  )

## change names using tech doc PDF:
doc17 <- pdftools::pdf_text("2017_Individual_Unit_file/2017 S&L Public Use Files Technical Documentation.pdf")
doc17 <- doc17[6:14]
doc17 <- unlist(str_split(doc17,pattern = "\n"))
doc17 <- doc17[doc17!=""]
doc17 <- doc17[grep("Item Code\\W",doc17):length(doc17)]
doc17 <- data.frame(item_code = doc17,stringsAsFactors = F) %>% as_tibble()
doc17 <- doc17[-1,]
doc17$item_code <- str_trim(doc17$item_code)
doc17$var_name <- str_replace(doc17$item_code,"(\\w{3})\\W+(\\w.*)","\\2")
doc17$item_code <- str_replace(doc17$item_code,"(\\w{3})\\W+(\\w.*)","\\1")
doc17 <- doc17 %>%
  mutate(var_name = str_replace_all(var_name,c(" " = ".", "-" = "", "&" = "", "," = "","/" = "","\\.\\."=".")))

names(findat_2017_wide)[which(names(findat_2017_wide) %in% doc17$item_code)] <- doc17$var_name[match(names(findat_2017_wide)[which(names(findat_2017_wide) %in% doc17$item_code)], doc17$item_code)]
names(findat_2017_wide)[grep("State.Veterans'.AssistanceCurrent.Op",names(findat_2017_wide))[2]] <- "State.Veterans'.AssistanceCurrent.Op2"


save(findat_2017_wide, file="IndFin_2017.RData")
# load("IndFin_2017.RData")

## --------- ##
#### 2018: ####
## --------- ##
findat_2018 <- read_fwf("2018_Individual_Unit_file/2018FinEstDAT_08202020modp_pu.txt",
                        col_positions = fwf_widths(widths = c(14,3,12,4,1),
                                                   col_names = c("govid_14","item_code","amount","YearData","imputation"))
)
cwalk_2018 <- read_fwf("2018_Individual_Unit_file/Fin_GID_2018.txt",
                       col_positions = fwf_widths(widths = c(14,64,35,2,3,5,9,2,7,2,2,2,4,2),
                                                  col_names = c("govid_14","Name","CountyName",
                                                                "fips_state","fips_county","fips_place",
                                                                "population","pop_year","enrollment","enrollment_year",
                                                                "function_code","school_lvl_code","FYEndDate","SurvYear")))
findat_2018 <- left_join(findat_2018,cwalk_2018,by="govid_14")
findat_2018 <- findat_2018 %>% 
  group_by(govid_14,item_code) %>%
  mutate(dupe = n()>1)
# findat_2018 %>% filter(dupe==1) %>% View() # should be none
findat_2018 <- findat_2018 %>%
  filter(!(is.na(imputation) & dupe==1)) # eliminate double entries in 3 counties for item code X08
findat_2018 <- findat_2018 %>% select(-imputation,-dupe) # ignore imputation

findat_2018_wide <- spread(findat_2018, key=item_code, value=amount)
# findat_2018_wide <- findat_2018_wide %>%
#   filter(imputation == "R") # eliminate imputed variables - no longer doing this
## check that this is right number of governments: 
# length(unique(findat_2018$govid_14[findat_2018$imputation=="R"])) # missing one?


## Aggregating data into broad categories using "publication aggregates" methodology file downloaded for 2014
findat_2018_wide <- findat_2018_wide %>%
  rowwise() %>%
  mutate(Total.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                             # A56, # Charges.Natural.Resources.Forestry missing b/c only at state-fed level
                             A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                             # U21, # missing b/c only at state-fed level
                             U30, U40, U41, U50, U95, U99, A90, A91, A92, A93, A94, 
                             # X01, X02, X05, X08, # Emp Ret exp no longer in for 2018
                             Y01, Y02, 
                             # Y04,  # no longer in as of 2016
                             Y11, Y12, Y51, Y52,na.rm=T),
         
         Total.Expenditure = sum(c(E01, E03, E04, E05, E12, E16, E18, E21, E22, E23, E24, E25,
                                   # E26, # Current.Operations.Legislative.Services missing b/c only at state-fed level
                                   E29, E31, E32, E36, E44, E45, E50, E52, 
                                   # E55, # Current.Operations.Natural.Resources.Fish.and.Game missing b/c only at state-fed level
                                   # E56, # Current.Operations.Natural.Resources.Forestry missing b/c only at state-fed level
                                   E59, E60, E61, E62, E66, 
                                   # E73, # missing b/c only at state-fed level
                                   E74, E75, E77, E79, E80, E81, E85, E87, E89, E90, E91, E92, E93, E94, I89, I91, I92, I93, I94, J19, L67, J68, J85, 
                                   # X11, X12, # Emp Ret no longer in as of 2018
                                   Y05, Y06, Y14, Y53, F01, F03, F04, F05, F12, F16, F18, F21, F22, F23, F24, F25, 
                                   # F26, # Construction.Legislative missing b/c only at state-fed level
                                   F29, F31, F32, F36, F44, F45, F50, F52,
                                   # F55, # Construction.Fish.and.Game missing b/c only at state-fed level
                                   # F56, # Construction.Forestry missing b/c only at state-fed level
                                   F59, F60, F61, F62, F66, F77, F79, F80, F81, F85, F87, F89, F90, F91, F92, F93, F94, G01, G03, G04, G05, G12, G16, G18, G21, G22, G23, G24, G25, 
                                   # G26, # Other.Capital.Outlay.Legislative missing b/c only at state-fed level
                                   G29, G31, G32, G36, G44, G45, G50, G52, 
                                   # G55, # Other.Capital.Outlay.Fish.and.Game missing b/c only at state-fed level
                                   # G56, # Other.Capital.Outlay.Forestry missing b/c only at state-fed level
                                   G59, G60, G61, G62, G66, G77, G79, G80, G81, G85, G87, G89, G90, G91, G92, G93, G94, J67, L01, L04, L05, L12, L18, L23, L25, L29, L32, L36, L44, L50, L52, L59, L60, L61, L62, L66, L67, L79, L80, L81, L87, L89, L91, L92, L93, L94, M01, M04, M05, M12, M18, M21, M23, M24, M25, M29, M30, M32, M36, M44, M50, M52, 
                                   # M55, # Intergovernmental.to.Local.NEC.Fish.and.Game missing b/c only at state-fed level
                                   # M56, # Intergovernmental.to.Local.NEC.Forestry missing b/c only at state-fed level
                                   M59, M60, M61, M62, M66, M67, M68, M79, M80, M81, M87, M89, M91, M92, M93, M94, Q12, Q18, S67, 
                                   # S74, # missing b/c only at state-fed level
                                   S89),na.rm=T),
         
         # exp categories:
         Direct.Expenditure = sum(E01, E03, E04, E05, E12, E16, E18, E21, E22, E23, E24, E25,
                                  # E26, 
                                  E29, E31, E32, E36, E44, E45, E50, E52, 
                                  # E55, E56,
                                  E59, E60, E61, E62, E66, 
                                  # E73,
                                  E74, E75, E77, E79, E80, E81, E85, E87, E89, E90, E91, E92, E93, E94, F01, F03, F04, F05, F12, F16, F18, F21, F22, F23, F24, F25,
                                  # F26, 
                                  F29, F31, F32, F36, F44, F45, F50, F52,
                                  # F55, F56, 
                                  F59, F60, F61, F62, F66, F77, F79, F80, F81, F85, F87, F89, F90, F91, F92, F93, F94, G01, G03, G04, G05, G12, G16, G18, G21, G22, G23, G24, G25, 
                                  # G26, 
                                  G29, G31, G32, G36, G44, G45, G50, G52, 
                                  # G55, G56,
                                  G59, G60, G61, G62, G66, G77, G79, G80, G81, G85, G87, G89, G90, G91, G92, G93, G94, 
                                  # X11, X12, # Emp Ret no longer in as of 2018
                                  Y05, Y06, Y14, Y53, J19, J67, J68, J85, I89, I91, I92, I93, I94,na.rm=T),
         
         Total.HospitalTotal.Exp = sum(E36, F36, G36,na.rm=T),
         CorrectTotal.Exp = sum(E04, F04, G04, E05, F05, G05,na.rm=T),
         Natural.ResTotal.Exp = sum(
           # E55, F55, G55, E56, F56, G56,
           E59, F59, G59,na.rm=T),
         Total.EducTotal.Exp = sum(E12, F12, G12, E16, F16, G16, E18, F18, G18, J19, E21, F21, G21,na.rm=T),
         General.NECTotal.Exp = sum(E89, F89, G89,na.rm=T), # called "other and unallocable" in methodology doc
         Fire.ProtTotal.Expend = sum(E24, F24, G24,na.rm=T),
         Police.ProtTotal.Exp = sum(E62, F62, G62,na.rm=T),
         HealthTotal.Expend = sum(E32, F32, G32,na.rm=T),
         Total.HighwaysTot.Exp = sum(E44, F44, G44, E45, F45, G45,na.rm=T),
         ParkingTotal.Expend = sum(E60, F60, G60,na.rm=T),
         Hous..ComTotal.Exp = sum(E50, F50, G50,na.rm=T),
         LibrariesTotal.Expend = sum(E52, F52, G52,na.rm=T),
         Parks..RecTotal.Exp = sum(E61, F61, G61,na.rm=T),
         SewerageTotal.Expend = sum(E80, F80, G80,na.rm=T),
         Total.UtilTotal.Exp = sum(E91, F91, G91, I91, E92, F92, G92, I92, E93, F93, G93, I93, E94, F94, G94, I94,na.rm=T),
         Public.WelfTotal.Exp = sum(J67, J68, 
                                    # E73, 
                                    E74, E75, E77, F77, G77, E79, F79, G79,na.rm=T),
         Total.Interest.on.Debt = sum(I89,na.rm=T),
         Fin.AdminTotal.Exp = sum(E23, F23, G23,na.rm=T),
         
         # debt categories
         Total.Debt.Outstanding = sum(`44T`, `49U`, `64V`,na.rm=T),
         LTD.OutGeneral = NA, # not sure; not in methodology doc
         Total.LTD.OutFFC = NA, # not sure; not in methodology doc
         Total.LTD.OutNG = NA, # not sure; not in methodology doc
         Total.LTD.Outstanding = sum(`44T`, `49U`,na.rm=T),
         
         # rev categories:
         General.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                               # A56,
                               A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                               # U21,
                               U30, U40, U41, U50, U95, U99,na.rm=T),
         General.RevOwn.Source = sum(A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                                     # A56, missing b/c only at state-fed level
                                     A59, A60, A61, A80, A81, A87, A89, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, U01, U11, U20, 
                                     # U21, missing b/c only at state-fed level
                                     U30, U40, U41, U50, U95, U99,na.rm=T),
         # Property.Tax = (T01,na.rm=T),
         Total.Taxes = sum(T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99,na.rm=T),
         Total.Select.Sales.Tax = sum(T10, T11, T12, T13, T14, T15, T16, T19,na.rm=T),
         Total.Gen.Sales.Tax = (T09),
         Total.State.IG.Revenue = sum(C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94,na.rm=T),
         Total.IG.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94,na.rm=T),
         
         Total.RevOwn.Sources = Total.Revenue - Total.IG.Revenue,
         
         Tot.Chgs.and.Misc.Rev = sum(A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                                     # A56, 
                                     A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                                     # U21, 
                                     U30, U40, U41, U50, U95, U99,na.rm=T),
         Total.Utility.Revenue = sum(A91, A92, A93, A94)
  )

## change names using tech doc PDF:
doc18 <- pdftools::pdf_text("2018_Individual_Unit_file/2018 S&L Public Use Files Technical Documentation.pdf")
doc18 <- doc18[6:14]
doc18 <- unlist(str_split(doc18,pattern = "\n"))
doc18 <- doc18[doc18!=""]
doc18 <- doc18[grep("Item Code\\W",doc18):length(doc18)]
doc18 <- data.frame(item_code = doc18,stringsAsFactors = F) %>% as_tibble()
doc18 <- doc18[-1,]
doc18$item_code <- str_trim(doc18$item_code)
doc18$var_name <- str_replace(doc18$item_code,"(\\w{3})\\W+(\\w.*)","\\2")
doc18$item_code <- str_replace(doc18$item_code,"(\\w{3})\\W+(\\w.*)","\\1")
doc18 <- doc18 %>%
  mutate(var_name = str_replace_all(var_name,c(" " = ".", "-" = "", "&" = "", "," = "","/" = "","\\.\\."=".")))

names(findat_2018_wide)[which(names(findat_2018_wide) %in% doc18$item_code)] <- doc18$var_name[match(names(findat_2018_wide)[which(names(findat_2018_wide) %in% doc18$item_code)], doc18$item_code)]
names(findat_2018_wide)[grep("State.Veterans'.AssistanceCurrent.Op",names(findat_2018_wide))[2]] <- "State.Veterans'.AssistanceCurrent.Op2"


save(findat_2018_wide, file="IndFin_2018.RData")
# load("IndFin_2018.RData")

## --------- ##
#### 2019: ####
## --------- ##
findat_2019 <- read_fwf("2019_Individual_Unit_file/2019FinEstDAT_06102021modp_pu.txt",
                        col_positions = fwf_widths(widths = c(12,3,12,4,1),
                                                   col_names = c("govid_14","item_code","amount","YearData","imputation"))
)
cwalk_2019 <- read_fwf("2019_Individual_Unit_file/Fin_PID_2019.txt",
                       col_positions = fwf_widths(widths = c(12,64,35,
                                                             # 2,3,
                                                             5,9,2,7,2,2,2,4,2),
                                                  col_names = c("govid_14","Name","CountyName",
                                                                # "fips_state","fips_county",
                                                                "fips_place",
                                                                "population","pop_year","enrollment","enrollment_year",
                                                                "function_code","school_lvl_code","FYEndDate","SurvYear")))
# didn't have state/county FIPS codes in 2019, creating:
cwalk_2019 <- cwalk_2019 %>%
  mutate(fips_state = str_replace(govid_14,"(\\d{2})(\\d{1})(\\d{3}).*","\\1"),
         fips_county = str_replace(govid_14,"(\\d{2})(\\d{1})(\\d{3}).*","\\3"))

findat_2019 <- left_join(findat_2019,cwalk_2019,by="govid_14")
findat_2019 <- findat_2019 %>% 
  group_by(govid_14,item_code) %>%
  mutate(dupe = n()>1)
# findat_2019 %>% filter(dupe==1) %>% View() # should be none
findat_2019 <- findat_2019 %>%
  filter(!(is.na(imputation) & dupe==1)) # eliminate double entries in 3 counties for item code X08
findat_2019 <- findat_2019 %>% select(-imputation,-dupe) # ignore imputation

findat_2019_wide <- spread(findat_2019, key=item_code, value=amount)
# findat_2019_wide <- findat_2019_wide %>%
#   filter(imputation == "R") # eliminate imputed variables - no longer doing this
## check that this is right number of governments: 
# length(unique(findat_2019$govid_14[findat_2019$imputation=="R"])) # missing one?


## Aggregating data into broad categories using "publication aggregates" methodology file downloaded for 2014
findat_2019_wide <- findat_2019_wide %>%
  rowwise() %>%
  mutate(Total.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                             # A56, # Charges.Natural.Resources.Forestry missing b/c only at state-fed level
                             A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                             # U21, # missing b/c only at state-fed level
                             U30, U40, U41, U50, U95, U99, A90, A91, A92, A93, A94, 
                             # X01, X02, X05, X08, # Emp Ret exp no longer in for 2019
                             Y01, Y02, 
                             # Y04,  # no longer in as of 2016
                             Y11, Y12, Y51, Y52,na.rm=T),
         
         Total.Expenditure = sum(c(E01, E03, E04, E05, E12, E16, E18, E21, E22, E23, E24, E25,
                                   # E26, # Current.Operations.Legislative.Services missing b/c only at state-fed level
                                   E29, E31, E32, E36, E44, E45, E50, E52, 
                                   # E55, # Current.Operations.Natural.Resources.Fish.and.Game missing b/c only at state-fed level
                                   # E56, # Current.Operations.Natural.Resources.Forestry missing b/c only at state-fed level
                                   E59, E60, E61, E62, E66, 
                                   # E73, # missing b/c only at state-fed level
                                   E74, E75, E77, E79, E80, E81, E85, E87, E89, E90, E91, E92, E93, E94, I89, I91, I92, I93, I94, J19, L67, J68, 
                                   # J85, # State Veterans' Assistance no longer in as of 2019
                                   # X11, X12, # Emp Ret no longer in as of 2018
                                   Y05, Y06, Y14, Y53, F01, F03, F04, F05, F12, F16, F18, F21, F22, F23, F24, F25, 
                                   # F26, # Construction.Legislative missing b/c only at state-fed level
                                   F29, F31, F32, F36, F44, F45, F50, F52,
                                   # F55, # Construction.Fish.and.Game missing b/c only at state-fed level
                                   # F56, # Construction.Forestry missing b/c only at state-fed level
                                   F59, F60, F61, F62, F66, F77, F79, F80, F81, F85, F87, F89, F90, F91, F92, F93, F94, G01, G03, G04, G05, G12, G16, G18, G21, G22, G23, G24, G25, 
                                   # G26, # Other.Capital.Outlay.Legislative missing b/c only at state-fed level
                                   G29, G31, G32, G36, G44, G45, G50, G52, 
                                   # G55, # Other.Capital.Outlay.Fish.and.Game missing b/c only at state-fed level
                                   # G56, # Other.Capital.Outlay.Forestry missing b/c only at state-fed level
                                   G59, G60, G61, G62, G66, G77, G79, G80, G81, G85, G87, G89, G90, G91, G92, G93, G94, J67, L01, L04, L05, L12, L18, L23, L25, L29, L32, L36, L44, L50, L52, L59, L60, L61, L62, L66, L67, L79, L80, L81, L87, L89, L91, L92, L93, L94, M01, M04, M05, M12, M18, M21, M23, M24, M25, M29, M30, M32, M36, M44, M50, M52, 
                                   # M55, # Intergovernmental.to.Local.NEC.Fish.and.Game missing b/c only at state-fed level
                                   # M56, # Intergovernmental.to.Local.NEC.Forestry missing b/c only at state-fed level
                                   M59, M60, M61, M62, M66, M67, M68, M79, M80, M81, M87, M89, M91, M92, M93, M94, Q12, Q18, S67, 
                                   # S74, # missing b/c only at state-fed level
                                   S89),na.rm=T),
         
         # exp categories:
         Direct.Expenditure = sum(E01, E03, E04, E05, E12, E16, E18, E21, E22, E23, E24, E25,
                                  # E26, 
                                  E29, E31, E32, E36, E44, E45, E50, E52, 
                                  # E55, E56,
                                  E59, E60, E61, E62, E66, 
                                  # E73,
                                  E74, E75, E77, E79, E80, E81, E85, E87, E89, E90, E91, E92, E93, E94, F01, F03, F04, F05, F12, F16, F18, F21, F22, F23, F24, F25,
                                  # F26, 
                                  F29, F31, F32, F36, F44, F45, F50, F52,
                                  # F55, F56, 
                                  F59, F60, F61, F62, F66, F77, F79, F80, F81, F85, F87, F89, F90, F91, F92, F93, F94, G01, G03, G04, G05, G12, G16, G18, G21, G22, G23, G24, G25, 
                                  # G26, 
                                  G29, G31, G32, G36, G44, G45, G50, G52, 
                                  # G55, G56,
                                  G59, G60, G61, G62, G66, G77, G79, G80, G81, G85, G87, G89, G90, G91, G92, G93, G94, 
                                  # X11, X12, # Emp Ret no longer in as of 2018
                                  Y05, Y06, Y14, Y53, J19, J67, J68, 
                                  # J85, # State Veterans' Assistance no longer in as of 2019
                                  I89, I91, I92, I93, I94,na.rm=T),
         
         Total.HospitalTotal.Exp = sum(E36, F36, G36,na.rm=T),
         CorrectTotal.Exp = sum(E04, F04, G04, E05, F05, G05,na.rm=T),
         Natural.ResTotal.Exp = sum(
           # E55, F55, G55, E56, F56, G56,
           E59, F59, G59,na.rm=T),
         Total.EducTotal.Exp = sum(E12, F12, G12, E16, F16, G16, E18, F18, G18, J19, E21, F21, G21,na.rm=T),
         General.NECTotal.Exp = sum(E89, F89, G89,na.rm=T), # called "other and unallocable" in methodology doc
         Fire.ProtTotal.Expend = sum(E24, F24, G24,na.rm=T),
         Police.ProtTotal.Exp = sum(E62, F62, G62,na.rm=T),
         HealthTotal.Expend = sum(E32, F32, G32,na.rm=T),
         Total.HighwaysTot.Exp = sum(E44, F44, G44, E45, F45, G45,na.rm=T),
         ParkingTotal.Expend = sum(E60, F60, G60,na.rm=T),
         Hous..ComTotal.Exp = sum(E50, F50, G50,na.rm=T),
         LibrariesTotal.Expend = sum(E52, F52, G52,na.rm=T),
         Parks..RecTotal.Exp = sum(E61, F61, G61,na.rm=T),
         SewerageTotal.Expend = sum(E80, F80, G80,na.rm=T),
         Total.UtilTotal.Exp = sum(E91, F91, G91, I91, E92, F92, G92, I92, E93, F93, G93, I93, E94, F94, G94, I94,na.rm=T),
         Public.WelfTotal.Exp = sum(J67, J68, 
                                    # E73, 
                                    E74, E75, E77, F77, G77, E79, F79, G79,na.rm=T),
         Total.Interest.on.Debt = sum(I89,na.rm=T),
         Fin.AdminTotal.Exp = sum(E23, F23, G23,na.rm=T),
         
         # debt categories
         Total.Debt.Outstanding = sum(`44T`, `49U`, `64V`,na.rm=T),
         LTD.OutGeneral = NA, # not sure; not in methodology doc
         Total.LTD.OutFFC = NA, # not sure; not in methodology doc
         Total.LTD.OutNG = NA, # not sure; not in methodology doc
         Total.LTD.Outstanding = sum(`44T`, `49U`,na.rm=T),
         
         # rev categories:
         General.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                               # A56,
                               A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                               # U21,
                               U30, U40, U41, U50, U95, U99,na.rm=T),
         General.RevOwn.Source = sum(A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                                     # A56, missing b/c only at state-fed level
                                     A59, A60, A61, A80, A81, A87, A89, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, U01, U11, U20, 
                                     # U21, missing b/c only at state-fed level
                                     U30, U40, U41, U50, U95, U99,na.rm=T),
         # Property.Tax = (T01,na.rm=T),
         Total.Taxes = sum(T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99,na.rm=T),
         Total.Select.Sales.Tax = sum(T10, T11, T12, T13, T14, T15, T16, T19,na.rm=T),
         Total.Gen.Sales.Tax = (T09),
         Total.State.IG.Revenue = sum(C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94,na.rm=T),
         Total.IG.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94,na.rm=T),
         
         Total.RevOwn.Sources = Total.Revenue - Total.IG.Revenue,
         
         Tot.Chgs.and.Misc.Rev = sum(A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                                     # A56, 
                                     A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                                     # U21, 
                                     U30, U40, U41, U50, U95, U99,na.rm=T),
         Total.Utility.Revenue = sum(A91, A92, A93, A94)
  )

## change names using tech doc PDF:
doc19 <- pdftools::pdf_text("2019_Individual_Unit_file/2019 S&L Public Use Files Technical DocumentationPID.pdf")
doc19 <- doc19[6:14]
doc19 <- unlist(str_split(doc19,pattern = "\n"))
doc19 <- doc19[doc19!=""]
doc19 <- doc19[grep("Item Code\\W",doc19):length(doc19)]
doc19 <- data.frame(item_code = doc19,stringsAsFactors = F) %>% as_tibble()
doc19 <- doc19[-1,]
doc19$item_code <- str_trim(doc19$item_code)
doc19$var_name <- str_replace(doc19$item_code,"(\\w{3})\\W+(\\w.*)","\\2")
doc19$item_code <- str_replace(doc19$item_code,"(\\w{3})\\W+(\\w.*)","\\1")
doc19 <- doc19 %>%
  mutate(var_name = str_replace_all(var_name,c(" " = ".", "-" = "", "&" = "", "," = "","/" = "","\\.\\."=".")))

names(findat_2019_wide)[which(names(findat_2019_wide) %in% doc19$item_code)] <- doc19$var_name[match(names(findat_2019_wide)[which(names(findat_2019_wide) %in% doc19$item_code)], doc19$item_code)]
# names(findat_2019_wide)[grep("State.Veterans'.AssistanceCurrent.Op",names(findat_2019_wide))[2]] <- "State.Veterans'.AssistanceCurrent.Op2"


save(findat_2019_wide, file="IndFin_2019.RData")
# load("IndFin_2019.RData")

## --------- ##
#### 2020: ####
## --------- ##
findat_2020 <- read_fwf("2020_Individual_Unit_file/2020FinEstDAT_05312022modp_pu.txt",
                        col_positions = fwf_widths(widths = c(12,3,12,4,1),
                                                   col_names = c("govid_14","item_code","amount","YearData","imputation"))
)
cwalk_2020 <- read_fwf("2020_Individual_Unit_file/Fin_PID_2020.txt",
                       col_positions = fwf_widths(widths = c(12,64,35,
                                                             # 2,3,
                                                             5,9,2,7,2,2,2,4,2),
                                                  col_names = c("govid_14","Name","CountyName",
                                                                # "fips_state","fips_county",
                                                                "fips_place",
                                                                "population","pop_year","enrollment","enrollment_year",
                                                                "function_code","school_lvl_code","FYEndDate","SurvYear")))
# didn't have state/county FIPS codes in 2020, creating:
cwalk_2020 <- cwalk_2020 %>%
  mutate(fips_state = str_replace(govid_14,"(\\d{2})(\\d{1})(\\d{3}).*","\\1"),
         fips_county = str_replace(govid_14,"(\\d{2})(\\d{1})(\\d{3}).*","\\3"))

findat_2020 <- left_join(findat_2020,cwalk_2020,by="govid_14")
findat_2020 <- findat_2020 %>% 
  group_by(govid_14,item_code) %>%
  mutate(dupe = n()>1)
# findat_2020 %>% filter(dupe==1) %>% View() # should be none
findat_2020 <- findat_2020 %>%
  filter(!(is.na(imputation) & dupe==1)) # eliminate double entries in 3 counties for item code X08
findat_2020 <- findat_2020 %>% select(-imputation,-dupe) # ignore imputation

findat_2020_wide <- spread(findat_2020, key=item_code, value=amount)
# findat_2020_wide <- findat_2020_wide %>%
#   filter(imputation == "R") # eliminate imputed variables - no longer doing this
## check that this is right number of governments: 
# length(unique(findat_2020$govid_14[findat_2020$imputation=="R"])) # missing one?


## Aggregating data into broad categories using "publication aggregates" methodology file downloaded for 2014
findat_2020_wide <- findat_2020_wide %>%
  rowwise() %>%
  mutate(Total.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                             # A56, # Charges.Natural.Resources.Forestry missing b/c only at state-fed level
                             A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                             # U21, # missing b/c only at state-fed level
                             U30, U40, U41, U50, U95, U99, A90, A91, A92, A93, A94, 
                             # X01, X02, X05, X08, # Emp Ret exp no longer in for 2020
                             Y01, Y02, 
                             # Y04,  # no longer in as of 2016
                             Y11, Y12, Y51, Y52,na.rm=T),
         
         Total.Expenditure = sum(c(E01, E03, E04, E05, E12, E16, E18, E21, E22, E23, E24, E25,
                                   # E26, # Current.Operations.Legislative.Services missing b/c only at state-fed level
                                   E29, E31, E32, E36, E44, E45, E50, E52, 
                                   # E55, # Current.Operations.Natural.Resources.Fish.and.Game missing b/c only at state-fed level
                                   # E56, # Current.Operations.Natural.Resources.Forestry missing b/c only at state-fed level
                                   E59, E60, E61, E62, E66, 
                                   # E73, # missing b/c only at state-fed level
                                   E74, E75, E77, E79, E80, E81, E85, E87, E89, E90, E91, E92, E93, E94, I89, I91, I92, I93, I94, J19, L67, J68, 
                                   # J85, # State Veterans' Assistance no longer in as of 2020
                                   # X11, X12, # Emp Ret no longer in as of 2018
                                   Y05, Y06, Y14, Y53, F01, F03, F04, F05, F12, F16, F18, F21, F22, F23, F24, F25, 
                                   # F26, # Construction.Legislative missing b/c only at state-fed level
                                   F29, F31, F32, F36, F44, F45, F50, F52,
                                   # F55, # Construction.Fish.and.Game missing b/c only at state-fed level
                                   # F56, # Construction.Forestry missing b/c only at state-fed level
                                   F59, F60, F61, F62, F66, F77, F79, F80, F81, F85, F87, F89, F90, F91, F92, F93, F94, G01, G03, G04, G05, G12, G16, G18, G21, G22, G23, G24, G25, 
                                   # G26, # Other.Capital.Outlay.Legislative missing b/c only at state-fed level
                                   G29, G31, G32, G36, G44, G45, G50, G52, 
                                   # G55, # Other.Capital.Outlay.Fish.and.Game missing b/c only at state-fed level
                                   # G56, # Other.Capital.Outlay.Forestry missing b/c only at state-fed level
                                   G59, G60, G61, G62, G66, G77, G79, G80, G81, G85, G87, G89, G90, G91, G92, G93, G94, J67, L01, L04, L05, L12, L18, L23, L25, L29, L32, L36, L44, L50, L52, L59, L60, L61, L62, L66, L67, L79, L80, L81, L87, L89, L91, L92, L93, L94, M01, M04, M05, M12, M18, M21, M23, M24, M25, M29, M30, M32, M36, M44, M50, M52, 
                                   # M55, # Intergovernmental.to.Local.NEC.Fish.and.Game missing b/c only at state-fed level
                                   # M56, # Intergovernmental.to.Local.NEC.Forestry missing b/c only at state-fed level
                                   M59, M60, M61, M62, M66, M67, M68, M79, M80, M81, M87, M89, M91, M92, M93, M94, Q12, Q18, S67, 
                                   # S74, # missing b/c only at state-fed level
                                   S89),na.rm=T),
         
         # exp categories:
         Direct.Expenditure = sum(E01, E03, E04, E05, E12, E16, E18, E21, E22, E23, E24, E25,
                                  # E26, 
                                  E29, E31, E32, E36, E44, E45, E50, E52, 
                                  # E55, E56,
                                  E59, E60, E61, E62, E66, 
                                  # E73,
                                  E74, E75, E77, E79, E80, E81, E85, E87, E89, E90, E91, E92, E93, E94, F01, F03, F04, F05, F12, F16, F18, F21, F22, F23, F24, F25,
                                  # F26, 
                                  F29, F31, F32, F36, F44, F45, F50, F52,
                                  # F55, F56, 
                                  F59, F60, F61, F62, F66, F77, F79, F80, F81, F85, F87, F89, F90, F91, F92, F93, F94, G01, G03, G04, G05, G12, G16, G18, G21, G22, G23, G24, G25, 
                                  # G26, 
                                  G29, G31, G32, G36, G44, G45, G50, G52, 
                                  # G55, G56,
                                  G59, G60, G61, G62, G66, G77, G79, G80, G81, G85, G87, G89, G90, G91, G92, G93, G94, 
                                  # X11, X12, # Emp Ret no longer in as of 2018
                                  Y05, Y06, Y14, Y53, J19, J67, J68, 
                                  # J85, # State Veterans' Assistance no longer in as of 2020
                                  I89, I91, I92, I93, I94,na.rm=T),
         
         Total.HospitalTotal.Exp = sum(E36, F36, G36,na.rm=T),
         CorrectTotal.Exp = sum(E04, F04, G04, E05, F05, G05,na.rm=T),
         Natural.ResTotal.Exp = sum(
           # E55, F55, G55, E56, F56, G56,
           E59, F59, G59,na.rm=T),
         Total.EducTotal.Exp = sum(E12, F12, G12, E16, F16, G16, E18, F18, G18, J19, E21, F21, G21,na.rm=T),
         General.NECTotal.Exp = sum(E89, F89, G89,na.rm=T), # called "other and unallocable" in methodology doc
         Fire.ProtTotal.Expend = sum(E24, F24, G24,na.rm=T),
         Police.ProtTotal.Exp = sum(E62, F62, G62,na.rm=T),
         HealthTotal.Expend = sum(E32, F32, G32,na.rm=T),
         Total.HighwaysTot.Exp = sum(E44, F44, G44, E45, F45, G45,na.rm=T),
         ParkingTotal.Expend = sum(E60, F60, G60,na.rm=T),
         Hous..ComTotal.Exp = sum(E50, F50, G50,na.rm=T),
         LibrariesTotal.Expend = sum(E52, F52, G52,na.rm=T),
         Parks..RecTotal.Exp = sum(E61, F61, G61,na.rm=T),
         SewerageTotal.Expend = sum(E80, F80, G80,na.rm=T),
         Total.UtilTotal.Exp = sum(E91, F91, G91, I91, E92, F92, G92, I92, E93, F93, G93, I93, E94, F94, G94, I94,na.rm=T),
         Public.WelfTotal.Exp = sum(J67, J68, 
                                    # E73, 
                                    E74, E75, E77, F77, G77, E79, F79, G79,na.rm=T),
         Total.Interest.on.Debt = sum(I89,na.rm=T),
         Fin.AdminTotal.Exp = sum(E23, F23, G23,na.rm=T),
         
         # debt categories
         Total.Debt.Outstanding = sum(`44T`, `49U`, `64V`,na.rm=T),
         LTD.OutGeneral = NA, # not sure; not in methodology doc
         Total.LTD.OutFFC = NA, # not sure; not in methodology doc
         Total.LTD.OutNG = NA, # not sure; not in methodology doc
         Total.LTD.Outstanding = sum(`44T`, `49U`,na.rm=T),
         
         # rev categories:
         General.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                               # A56,
                               A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                               # U21,
                               U30, U40, U41, U50, U95, U99,na.rm=T),
         General.RevOwn.Source = sum(A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                                     # A56, missing b/c only at state-fed level
                                     A59, A60, A61, A80, A81, A87, A89, T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99, U01, U11, U20, 
                                     # U21, missing b/c only at state-fed level
                                     U30, U40, U41, U50, U95, U99,na.rm=T),
         # Property.Tax = (T01,na.rm=T),
         Total.Taxes = sum(T01, T09, T10, T11, T12, T13, T14, T15, T16, T19, T20, T21, T22, T23, T24, T25, T27, T28, T29, T40, T41, T50, T51, T53, T99,na.rm=T),
         Total.Select.Sales.Tax = sum(T10, T11, T12, T13, T14, T15, T16, T19,na.rm=T),
         Total.Gen.Sales.Tax = (T09),
         Total.State.IG.Revenue = sum(C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94,na.rm=T),
         Total.IG.Revenue = sum(B01, B21, B22, B30, B42, B46, B50, B59, B79, B80, B89, B91, B92, B93, B94, C21, C30, C42, C46, C50, C79, C80, C89, C91, C92, C93, C94, D21, D30, D42, D46, D50, D79, D80, D89, D91, D92, D93, D94,na.rm=T),
         
         Total.RevOwn.Sources = Total.Revenue - Total.IG.Revenue,
         
         Tot.Chgs.and.Misc.Rev = sum(A01, A03, A09, A10, A12, A16, A18, A21, A36, A44, A45, A50, 
                                     # A56, 
                                     A59, A60, A61, A80, A81, A87, A89, U01, U11, U20, 
                                     # U21, 
                                     U30, U40, U41, U50, U95, U99,na.rm=T),
         Total.Utility.Revenue = sum(A91, A92, A93, A94)
  )

## change names using tech doc PDF:
doc20 <- pdftools::pdf_text("2020_Individual_Unit_file/2020 S&L Public Use Files Technical Documentation.pdf")
doc20 <- doc20[6:14]
doc20 <- unlist(str_split(doc20,pattern = "\n"))
doc20 <- doc20[doc20!=""]
doc20 <- doc20[grep("Item Code\\W",doc20):length(doc20)]
doc20 <- data.frame(item_code = doc20,stringsAsFactors = F) %>% as_tibble()
doc20 <- doc20[-1,]
doc20$item_code <- str_trim(doc20$item_code)
doc20$var_name <- str_replace(doc20$item_code,"(\\w{3})\\W+(\\w.*)","\\2")
doc20$item_code <- str_replace(doc20$item_code,"(\\w{3})\\W+(\\w.*)","\\1")
doc20 <- doc20 %>%
  mutate(var_name = str_replace_all(var_name,c(" " = ".", "-" = "", "&" = "", "," = "","/" = "","\\.\\."=".")))

names(findat_2020_wide)[which(names(findat_2020_wide) %in% doc20$item_code)] <- doc20$var_name[match(names(findat_2020_wide)[which(names(findat_2020_wide) %in% doc20$item_code)], doc20$item_code)]
# names(findat_2020_wide)[grep("State.Veterans'.AssistanceCurrent.Op",names(findat_2020_wide))[2]] <- "State.Veterans'.AssistanceCurrent.Op2"


save(findat_2020_wide, file="IndFin_2020.RData")
# load("IndFin_2020.RData")


## -------------------------- ##
#### Assemble long dataset: ####
## -------------------------- ##
load("IndFin_2013.RData")
load("IndFin_2014.RData")
load("IndFin_2015.RData")
load("IndFin_2016.RData")
load("IndFin_2017.RData")
findat_2017_wide$enrollment <- as.numeric(findat_2017_wide$enrollment)
load("IndFin_2018.RData")
load("IndFin_2019.RData")
load("IndFin_2020.RData")
load("IndFin_19672012.RData")

## combine all years:
IndFin <- bind_rows(IndFin, findat_2013_wide)
IndFin <- bind_rows(IndFin, findat_2014_wide)
IndFin <- bind_rows(IndFin, findat_2015_wide)
IndFin <- bind_rows(IndFin, findat_2016_wide)
IndFin <- bind_rows(IndFin, findat_2017_wide)
IndFin <- bind_rows(IndFin, findat_2018_wide)
IndFin <- bind_rows(IndFin, findat_2019_wide)
IndFin <- bind_rows(IndFin, findat_2020_wide)

IndFin <- IndFin %>%
  mutate(Year4 = coalesce(Year4,YearData),
         Population = coalesce(Population,population),
         Year.of.Pop.Data = coalesce(Year.of.Pop.Data, as.character(pop_year)))

save(IndFin,file="IndFin_19702020_nopop.RData",compress = "gzip")
# load(file="IndFin_19702020_nopop.RData")

## ----------------------------- ##
#### Population interpolation: ####
## ----------------------------- ##

## The following large section of code
## interpolates population based on actual census data.

# IndFin <- subset(IndFin, !is.na(Year4)) # not needed

population_data <- IndFin %>% 
  select(govid_14,Year.of.Pop.Data,Year4,Name,Population)

population_data <- population_data %>%
  mutate(Year.of.Pop.Data = ifelse(as.numeric(Year.of.Pop.Data)>25, 
                                   paste0("19",Year.of.Pop.Data),
                                   paste0("20",Year.of.Pop.Data))
         )
population_data$Year.of.Pop.Data[which(population_data$Year.of.Pop.Data=="208")] <- "2008"

population_data$Year.of.Pop.Data[is.na(population_data$Year.of.Pop.Data)] <- population_data$Year4[is.na(population_data$Year.of.Pop.Data)]
population_data$Year.of.Pop.Data <- as.numeric(population_data$Year.of.Pop.Data)

# population_data$YearPop <- NA
# temp <- as.numeric(as.vector(population_data$Year.of.Pop.Data))
# 
# population_data$YearPop[temp>14 & !is.na(temp)] <- paste("19",
#          population_data$Year.of.Pop.Data[temp>14 & !is.na(temp)], sep="")
# 
# 
# population_data$YearPop[temp<10 & !is.na(temp)]<-paste( "200",
#          as.numeric(as.vector(population_data$Year.of.Pop.Data[temp<10 & !is.na(temp)])), sep="")
# 
# 
# population_data$YearPop[temp>9 & temp<14 & !is.na(temp)]<-paste("20",
#          as.numeric(as.vector(population_data$Year.of.Pop.Data[temp>9 & temp<14 & !is.na(temp)])), sep="")


# population_data$YearPop[is.na(population_data$YearPop)] <- population_data$YearData[is.na(population_data$YearPop)]

population_data <- population_data %>%
  filter(!is.na(Population)) %>%
  select(-Year4) %>%
  group_by(govid_14) %>%
  complete(Year.of.Pop.Data = seq.int(1970, 2020,by = 1)) %>% # expand to have all years
  fill(Name) %>% # fill invariant variables down
  distinct(govid_14,Year.of.Pop.Data,Name,.keep_all = T) 

population_data <- population_data %>%
  mutate(population_est = Population)

## Need to remove gov units which have *all* NA values of population:
population_data_nonNA <- population_data %>%
  group_by(govid_14) %>%
  summarize(sum_notna_pop = sum(!is.na(population_est)))

population_data_nonNA <- population_data_nonNA %>%
  filter(sum_notna_pop>0)

population_data <- population_data %>%
  filter(!is.na(Name) & (govid_14 %in% population_data_nonNA$govid_14))

## Now interpolate between years with LOCF rule for ending years
population_data <- population_data %>%
  group_by(govid_14) %>%
  arrange(Year.of.Pop.Data) %>%
  mutate(population_est = zoo::na.approx(population_est,rule=2))
# this above line won't run if you set maxgap argument or na.rm - not sure why

# testing:
# population_data %>% filter(govid_14=="515021201") %>% mutate(population_est_new = zoo::na.approx(population_est,x=Year.of.Pop.Data,na.rm=F,rule=2,maxgap=11))
# pop_data2 <- population_data %>% filter(govid_14=="515021201") %>% arrange(Year.of.Pop.Data) %>% as.data.frame()
# pop_data2 <- mutate(pop_data2, population_est_new = zoo::na.approx(population_est,,x=Year.of.Pop.Data,rule=2))


cor(population_data$Population,population_data$population_est,use = "complete.obs") # 1, meaning it worked
sum(is.na(population_data$population_est)) # 0!
table(population_data$Year.of.Pop.Data,is.na(population_data$population_est)) # 

write_csv(population_data,path = "population_data.csv")
population_data <- read_csv(file="population_data.csv")


population_data <- population_data %>%
  select(govid_14,Year.of.Pop.Data,population_est) %>%
  distinct()

population_data <- population_data %>%
  filter(!is.na(govid_14))

nrow(IndFin) # 1428621

IndFin <- IndFin %>%
  filter(!is.na(govid_14) & !is.na(Year4))

nrow(IndFin) # 865559

IndFin2 <- left_join(IndFin, population_data,
                    by=c("govid_14" = "govid_14", "Year4" = "Year.of.Pop.Data")) # merge in correct year of pop
nrow(IndFin2) # 865590

cor(IndFin2$population_est, IndFin2$Population, use="complete.obs") # should be 1, except for years that had population data in wrong year

sum(is.na(IndFin2$population_est)) # 156954, or 16% have no population estimate data
table(IndFin2$Year4[is.na(IndFin2$population_est)]) # all in 2013-2019... these must be govts that didn't report in previous years at all?
table(IndFin2$Year4,is.na(IndFin2$population_est)) # have good coverage pre-2013

## --------------------------------------------------- ##
#### Population data: based on intercensal estimates ####
## --------------------------------------------------- ##
population_data_ic <- read_csv("../../../../Local Elections Data/Data/postcensal_population_1990-2020.csv") %>% 
  select(state,place,place_name,year,population)


population_data_ic <- population_data_ic %>%
  group_by(state,place) %>%
  complete(year = seq.int(1990, 2020,by = 1)) %>% # expand to have all years
  fill(place_name) %>% # fill invariant variables down
  distinct(state,place,place_name,year,.keep_all = T) 

population_data_ic <- population_data_ic %>%
  mutate(population_est = population,
         place_fips = paste0(str_pad(state,width = 2,side = "left",pad = "0"),str_pad(place,width = 5,side = "left",pad = "0"))) %>%
  filter(!is.na(place)) # only cities with valid place FIPS code

## Need to remove gov units which have *all* NA values of population:
population_data_nonNA <- population_data_ic %>%
  group_by(place_fips) %>%
  summarize(sum_notna_pop = sum(!is.na(population_est)))

population_data_nonNA <- population_data_nonNA %>%
  filter(sum_notna_pop>0)

population_data_ic <- population_data_ic %>%
  filter(place_fips %in% population_data_nonNA$place_fips)

## Now interpolate between years with LOCF rule for ending years
population_data_ic <- population_data_ic %>%
  group_by(place_fips) %>%
  arrange(year) %>%
  mutate(population_est = zoo::na.approx(population_est,rule=2))
# this above line won't run if you set maxgap argument or na.rm - not sure why

population_data_ic <- population_data_ic %>%
  mutate(place_fips = as.numeric(place_fips))

# merge in govid_14 codes
cwalk <- readxl::read_xls("_IndFin_1967-2012/GOVS_ID_to_FIPS_Place_Codes_2002.xls",skip = 16)
names(cwalk) <- c("govid_9","govid_14","gov_state_code","gov_type_code","gov_county_code",
                  "Name","CountyName","fips_state","fips_county","fips_place")
cwalk_for_merge <- cwalk %>%
  mutate(place_fips = as.numeric(paste0(str_pad(fips_state,width = 2,side = "left",pad = "0"),str_pad(fips_place,width = 5,side = "left",pad = "0")))) %>%
#   # filter(gov_type==2) %>%
  select(place_fips,govid_14) %>%
  filter(!is.na(place_fips))

nrow(population_data_ic) # 632023
population_data_ic <- left_join(population_data_ic,cwalk_for_merge,by=("place_fips"))
nrow(population_data_ic) # 632023


cor(population_data_ic$population,population_data_ic$population_est,use = "complete.obs") # 1, meaning it worked
sum(is.na(population_data_ic$population_est)) # 0, worked
table(population_data_ic$year,is.na(population_data_ic$population_est)) # to check

population_data_ic <- population_data_ic %>%
  rename(population_ic = population_est) %>%
  ungroup()

write_csv(population_data_ic,path = "population_data_intercensal_imputed.csv")
# population_data_ic <- read_csv("population_data_intercensal_imputed.csv")





## merge these pop estimates back to IndFin data:
mean(is.na(IndFin2$govid_14)) # 0
mean(is.na(population_data_ic$govid_14)) # 0.0166
nrow(population_data_ic) # 632023
n_distinct(select(population_data_ic,govid_14,year)) # 621534
population_data_ic %>% group_by(govid_14,year) %>% filter(n()>1) %>% ungroup() %>% select(govid_14) %>% distinct() %>% View() # these are all places with NA for govid_14 - small CDPs, etc.
population_data_ic <- filter(population_data_ic,!is.na(govid_14))

# library(panelView)
# library(viridis) 
# library(RColorBrewer)
# mycol <- brewer.pal(3,"Set2")
# mycol <- viridis(2,option = "viridis")
# panelView(D = "population_ic", data = data.frame(subset(population_data_ic,!is.na(govid_14))),
#           index = c("govid_14","year"), type="treat",
#           # main = "Delta Log(Monthly Violent Crimes)",
#           main="",
#           ylab="City",xlab="Year",
#           gridOff=T, 
#           outcome.type="continuous",
#           # color = mycol,
#           axis.lab.gap=c(12,0),
#           axis.adjust = T,
#           legend.labs = "population:",
#           background = "white")


nrow(IndFin2) # 889872
IndFin2 <- left_join(IndFin2, select(population_data_ic,govid_14,year,population_ic),
                     by=c("govid_14" = "govid_14", "Year4" = "year")) # merge in correct year of pop
nrow(IndFin2) # 889872

### reconcile two population counts:
IndFin2 <- IndFin2 %>%
  mutate(population_est_cog = population_est,
         population_est = NA,
         population_est = coalesce(population_ic,population_est_cog))
mean(is.na(IndFin2$population_est)) # 19.65% - these will be non-places (i.e. non-cities)

# IndFin2 <- filter(IndFin2, !is.na(population_est))

## Drop city/years with imputed fiscal data
## see email from Joseph Dalaker at census (below):
# Actual year of data reported (e.g., 90, 87, or one of these codes:)........................
# BB = Field in source file was blank (or not usable)
# CC = Data pulled from previous census of governments
# DD = Data provided by government (i.e., not imputed)
# GG = Data from census of governments' directory survey
#    II     = Data were imputed
#    NN = Not available       
#    For 1997 and later surveys, a suffix of "G", "P", or "Q" may be appended
#      to the year indicating that the data were imputed using various methods
#      based on the data for the year cited.  ("R" = reported data)
#      Examples: 92G, 97Q, 98P, 01R

# IndFin2 <- subset(IndFin2, YearofData!="II" & YearofData!="BB" & YearofData!="CC") # these are already gone, plus this would take out new years where this is NA

table(IndFin2$Year4)

# data$STATEAB <- as.vector(data$STATEAB)
# data$city <- as.vector(data$city)
# data$STATEAB <- as.vector(data$STATEAB)
# data <- merge(data,constraints,  by.x=c("NAME"), by.y=c("state"),
#             all.x=T)


IndFin2$city <- gsub( " TOWNSHIP", "",IndFin2$Name)
IndFin2$city <- gsub( " TOWN", "",IndFin2$city)
IndFin2$city <- gsub( " CITY AND BOROUGH", "",IndFin2$city)
IndFin2$city <- gsub( " CITY AND COUNTY", "",IndFin2$city)
IndFin2$city <- gsub( " CITY-PARISH", "",IndFin2$city)
IndFin2$city <- gsub( " CITY", "",IndFin2$city)
IndFin2$city <- gsub( " VILLAGE", "",IndFin2$city)
IndFin2$city <- gsub( "ST ", "St. ",IndFin2$city)
IndFin2$city <- gsub( "FT ", "Fort ",IndFin2$city)
IndFin2$city <- gsub( "MT ", "Mount ",IndFin2$city)
IndFin2$city <- gsub( "WASHINGTON DC", "Washington",IndFin2$city)
IndFin2$city <- gsub( "WILKES BARRE", "WILKES-BARRE",IndFin2$city)
IndFin2$city <- gsub( "WINSTON SALEM", "WINSTON-SALEM",IndFin2$city)
IndFin2$city <- gsub( " MUNICIPALITY", "",IndFin2$city)
IndFin2$city <- tolower(IndFin2$city)

IndFin2$city <- gsub( "lexington-fayette urban co govt", "lexington",IndFin2$city)
IndFin2$city <- gsub( "lexington-fayette urban county", "lexington",IndFin2$city)
IndFin2$city <- gsub( "lexington-fayette urban county g", "lexington",IndFin2$city)
IndFin2$city <- gsub( "lexington-fayette urban county government",
                "lexington",IndFin2$city)


## --------------------------- ##
#### Create percent measures ####
## --------------------------- ##

IndFin2 <- IndFin2 %>%
  mutate(
    # expenditure categories:
    education.perc = (Total.EducTotal.Exp/Total.Expenditure),
    fire.perc = (Fire.ProtTotal.Expend/Total.Expenditure),
    police.perc = (Police.ProtTotal.Exp/Total.Expenditure),
    health.perc = (HealthTotal.Expend/Total.Expenditure),
    highways.perc = (Total.HighwaysTot.Exp/Total.Expenditure),
    housing.perc = (Hous..ComTotal.Exp/Total.Expenditure),
    hospitals.perc = (Total.HospitalTotal.Exp/Total.Expenditure),
    corrections.perc = (CorrectTotal.Exp/Total.Expenditure),
    naturalresources.perc = (Natural.ResTotal.Exp/Total.Expenditure),
    libraries.perc = (LibrariesTotal.Expend/Total.Expenditure),
    parks.perc = (Parks..RecTotal.Exp/Total.Expenditure),
    sanitation.perc = (SewerageTotal.Expend/Total.Expenditure),
    utilities.perc = (Total.UtilTotal.Exp/Total.Expenditure),
    welfare.perc = (Public.WelfTotal.Exp/Total.Expenditure),
    interest.perc = (Total.Interest.on.Debt/Total.Expenditure),
    #     nonhighwaytransit.perc = (NonHighway.Transp.Total.Expend/Total.Expenditure),
    parking.perc = (ParkingTotal.Expend/Total.Expenditure),
    admin.perc = (Fin.AdminTotal.Exp/Total.Expenditure),
    nec.perc = (General.NECTotal.Exp/Total.Expenditure),
    
    salestax.perc = Tot.Sales..Gr.Rec.Tax/Total.Taxes,
    propertytax.perc = Property.Tax/Total.Taxes         
    
  )


## ------------------------------ ##
#### Create per capita measures ####
## ------------------------------ ##

## Adjust to 2019 dollars:
## Bring in CPI data from BLS, series CUUR0000SA0 retrieved here: https://data.bls.gov/timeseries/CUUR0000SA0
## Export to excel with "include annual averages" checkbox ticked
cpi <- readxl::read_excel("cpi_19502021.xlsx", na="") 
cpi <- mutate(cpi,
              cpi2020_multiplier = (cpi$Annual[cpi$Year==2020])/Annual,
              cpi2019_multiplier = (cpi$Annual[cpi$Year==2019])/Annual,
              cpi2017_multiplier = (cpi$Annual[cpi$Year==2017])/Annual,
              cpi2015_multiplier = (cpi$Annual[cpi$Year==2015])/Annual
)
cpi <- cpi %>%
  select(Year,cpi2015_multiplier,cpi2017_multiplier,cpi2019_multiplier,cpi2020_multiplier)
IndFin2 <- left_join(IndFin2, cpi, by=c("Year4" = "Year"))


### Expenditure:
# fixing some early years:
# IndFin2$SanitationTot.Exp[IndFin2$Year4>1969] <- IndFin2$SewerageTotal.Expend[IndFin2$Year4>1969]
IndFin2$SanitationTot.Exp <- IndFin2$SewerageTotal.Expend # since only post-1970
IndFin2$Fin.AdminTotal.Exp[IndFin2$Year4<2013] <- IndFin2$Fin.AdminTotal.Exp[IndFin2$Year4<2013] + 
  IndFin2$JudicialTotal.Expend[IndFin2$Year4<2013] + 
  IndFin2$Cen.StaffTotal.Expend[IndFin2$Year4<2013] + 
  IndFin2$Gen.Pub.BldgConstruct[IndFin2$Year4<2013]

IndFin2 <- IndFin2 %>%
  mutate(expenditure.PC = Total.Expenditure*cpi2019_multiplier/population_est*1000,
         generalexpenditure.PC = General.Expenditure*cpi2019_multiplier/population_est*1000,
         hospitals.PC = Total.HospitalTotal.Exp*cpi2019_multiplier/population_est*1000,
         corrections.PC = CorrectTotal.Exp*cpi2019_multiplier/population_est*1000,
         naturalresources.PC = Natural.ResTotal.Exp*cpi2019_multiplier/population_est*1000,
         education.PC = Total.EducTotal.Exp*cpi2019_multiplier/population_est*1000,
         nec.PC = General.NECTotal.Exp*cpi2019_multiplier/population_est*1000,
         fire.PC = Fire.ProtTotal.Expend*cpi2019_multiplier/population_est*1000,
         police.PC = Police.ProtTotal.Exp*cpi2019_multiplier/population_est*1000,
         health.PC = HealthTotal.Expend*cpi2019_multiplier/population_est*1000,
         highways.PC = Total.HighwaysTot.Exp*cpi2019_multiplier/population_est*1000,
         parking.PC = ParkingTotal.Expend*cpi2019_multiplier/population_est*1000,
         housing.PC = Hous..ComTotal.Exp*cpi2019_multiplier/population_est*1000,
         libraries.PC = LibrariesTotal.Expend*cpi2019_multiplier/population_est*1000,
         parks.PC = Parks..RecTotal.Exp*cpi2019_multiplier/population_est*1000,
         sanitation.PC = SanitationTot.Exp*cpi2019_multiplier/population_est*1000,
         utilities.PC = Total.UtilTotal.Exp*cpi2019_multiplier/population_est*1000,
         welfare.PC = Public.WelfTotal.Exp*cpi2019_multiplier/population_est*1000,
         interest.PC = Total.Interest.on.Debt*cpi2019_multiplier/population_est*1000,
         admin.PC = Fin.AdminTotal.Exp*cpi2019_multiplier/population_est*1000
         )

IndFin2$hospitals.PC[is.na(IndFin2$hospitals.PC)] <- 0


### Revenue:
# fixing some early years:
# IndFin2$Total.RevOwn.Sources[is.na(IndFin2$Total.RevOwn.Sources)] <- IndFin2$Total.Revenue[is.na(IndFin2$Total.RevOwn.Sources)]-IndFin2$Total.IG.Revenue[is.na(data$Total.RevOwn.Sources)] # not needed
# IndFin2$Property.Tax[IndFin2$Year4<=1969] <- IndFin2$Property.Tax..T01.[IndFin2$Year4<=1969] # not in these data...
# IndFin2$Total.Gen.Sales.Tax[IndFin2$Year4<=2006] <- IndFin2$Total.General.Sales.Taxes[IndFin2$Year4<=2006]

IndFin2 <- IndFin2 %>%
  mutate(revenue.PC = Total.Revenue*cpi2019_multiplier/population_est*1000,
         revenueown.PC = Total.RevOwn.Sources*cpi2019_multiplier/population_est*1000,
         revenuegeneral.PC = General.Revenue*cpi2019_multiplier/population_est*1000,
         revenueowngeneral.PC = Gen.RevOwn.Sources*cpi2019_multiplier/population_est*1000,
         Total.Taxes.PC = Total.Taxes*cpi2019_multiplier/population_est*1000,
         propertytax.PC = Property.Tax*cpi2019_multiplier/population_est*1000,
         salestax.select.PC = Total.Select.Sales.Tax*cpi2019_multiplier/population_est*1000,
         salestax.general.PC = Total.Gen.Sales.Tax*cpi2019_multiplier/population_est*1000,
         igrev.state.PC = Total.State.IG.Revenue*cpi2019_multiplier/population_est*1000,
         igrev.PC = Total.IG.Revenue*cpi2019_multiplier/population_est*1000,
         # FTE.PC = Total.Full.Time.Equivalent*cpi2019_multiplier/population_est*1000,
         # employees.PC =  = Total.Full.Time.Employees*cpi2019_multiplier/population_est*1000,
         debt.PC = Total.Debt.Outstanding*cpi2019_multiplier/population_est*1000,
         generaldebt.PC = LTD.OutGeneral*cpi2019_multiplier/population_est*1000,
         charges.PC = Tot.Chgs.and.Misc.Rev*cpi2019_multiplier/population_est*1000,
         utilitiesrev.PC = Total.Utility.Revenue*cpi2019_multiplier/population_est*1000,
         LT.debt.ffc.PC = Total.LTD.OutFFC*cpi2019_multiplier/population_est*1000,
         LT.debt.ng.PC = Tot.LTD.OutNG*cpi2019_multiplier/population_est*1000,
         LT.debt.PC = Total.LTD.Outstanding*cpi2019_multiplier/population_est*1000
  )

IndFin2$revenueown.PC[IndFin2$revenueown.PC==0] <- NA
IndFin2$revenue.PC[IndFin2$revenue.PC==0] <- NA
IndFin2$igrev.PC[IndFin2$revenue.PC==0] <- NA

IndFin2$fips_place[IndFin2$govid_14=="43201900300000"] <- "52006" # not sure why, but Nashville is 52004 here
IndFin2$fips_place[IndFin2$govid_14=="15204900800000"] <- "36000" # Fixing Indianapolis for continuity across 2012-2013
IndFin2$fips_place[IndFin2$govid_14=="11212100100000"] <- "04204" # Fixing Augusta GA for continuity across 2012-2013

## checking:
table(is.na(IndFin2$expenditure.PC),IndFin2$Year4)

write_rds(IndFin2,file = "IndFin_19672020_merged_icpop.rds",compress = "gz") # new version with population from the Census Bureau's intercensal estimates rather than COG estimates
# IndFin2 <- read_rds(file = "IndFin_19672020_merged_icpop.rds")


## Create deltas: --------------------------------------------------------------
assemble_finances <- F
# assemble_finances <- T
if(assemble_finances){
  IndFin2 <- read_rds("IndFin_19672020_merged_icpop.rds")
  
  # IndFin2$fips_place[IndFin2$govid_14=="43201900300000"] <- "52006" # not sure why, but Nashville is 52004 here - already fixed
  IndFin2$fips_place[IndFin2$govid_14=="15204900800000"] <- "36000" # Fixing Indianapolis for continuity across 2012-2013
  IndFin2$fips_place[IndFin2$govid_14=="11212100100000"] <- "04204" # Fixing Augusta GA for continuity across 2012-2013
  IndFin_formerge <- IndFin2 %>%
    select(Year4,govid_14,Type.Code,fips_state,fips_county,fips_place,County,Name,city,
           population_est,cpi2015_multiplier,cpi2017_multiplier,cpi2019_multiplier,cpi2020_multiplier,
           
           Total.Expenditure, Total.Revenue, Total.Taxes, General.Expenditure,
           education.perc, fire.perc, police.perc, health.perc, hospitals.perc, highways.perc,
           housing.perc, corrections.perc, libraries.perc, parks.perc, sanitation.perc, 
           naturalresources.perc, utilities.perc, welfare.perc, interest.perc,
           # nonhighwaytransit.perc ,
           parking.perc, admin.perc, nec.perc,
           
           salestax.perc, propertytax.perc,     
           
           expenditure.PC, generalexpenditure.PC, hospitals.PC,corrections.PC,naturalresources.PC, education.PC, nec.PC, fire.PC, police.PC,
           health.PC, highways.PC, parking.PC, housing.PC, libraries.PC, parks.PC,
           sanitation.PC, utilities.PC, welfare.PC,interest.PC, admin.PC, 
           
           revenue.PC, revenueown.PC, revenuegeneral.PC, revenueowngeneral.PC,
           Total.Taxes.PC,propertytax.PC, salestax.select.PC, salestax.general.PC, igrev.state.PC, igrev.PC,
           charges.PC, utilitiesrev.PC,
           
           debt.PC, generaldebt.PC, LT.debt.ffc.PC, LT.debt.ng.PC, LT.debt.PC
    ) 
  
  rm(IndFin2) # save memory
  gc() # save memory
  
  # cwalk <- read_csv("../../City councils/Cities/Data/GOVS_ID_to_FIPS_Place_Codes_2002_edited.csv") # should already be in there
  # cwalk_for_merge <- cwalk %>%
  #   mutate(place_fips = as.numeric(fips)) %>%
  #   # filter(gov_type==2) %>%
  #   select(govid_14,place_fips)
  # IndFin_formerge <- left_join(IndFin_formerge,cwalk_for_merge,by=("govid_14"))
  IndFin_formerge <- IndFin_formerge %>%
    mutate(place_fips = paste0(fips_state,fips_place))
  
  
  IndFin_formerge <- IndFin_formerge %>%
    mutate(expenditures.PC.nohospitals = expenditure.PC - hospitals.PC) 
  
  # testing:
  # View(IndFin_formerge %>% filter(place_fips=="3457000" | place_fips =="3402080") %>%
  #   group_by(place_fips) %>%
  #   complete(Year4 = seq.int(1970, 2017,by = 1)))
  
  IndFin_formerge <- IndFin_formerge %>%
    group_by(place_fips) %>%
    complete(Year4 = seq.int(1970, 2020,by = 1)) %>% # expand to have all years so can get leads/lags even if values==NA in that year
    fill(govid_14,Type.Code,fips_state,fips_county,fips_place,County,Name,city) %>% # fill invariant variables down
    mutate(
      ## Expenditures:
      expenditure.PC_lag3 = lag.new(expenditure.PC,n=3,along_with=Year4),
      expenditure.PC_lag2 = lag.new(expenditure.PC,n=2,along_with=Year4),
      expenditure.PC_lag1 = lag.new(expenditure.PC,n=1,along_with=Year4),
      expenditure.PC_lead1 = lead.new(expenditure.PC,n=1,along_with=Year4),
      expenditure.PC_lead2 = lead.new(expenditure.PC,n=2,along_with=Year4),
      expenditure.PC_lead3 = lead.new(expenditure.PC,n=3,along_with=Year4),
      expenditure.PC_lead4 = lead.new(expenditure.PC,n=4,along_with=Year4),
      expenditure.PC_lead5 = lead.new(expenditure.PC,n=5,along_with=Year4),
      expenditure.PC_delta1 = expenditure.PC_lead1 - expenditure.PC,
      expenditure.PC_delta2 = expenditure.PC_lead2 - expenditure.PC,
      expenditure.PC_delta3 = expenditure.PC_lead3 - expenditure.PC,
      
      expenditure.PC_ln = log1p(expenditure.PC),
      expenditure.PC_ln_lag3 = lag.new(expenditure.PC_ln,n=3,along_with=Year4),
      expenditure.PC_ln_lag2 = lag.new(expenditure.PC_ln,n=2,along_with=Year4),
      expenditure.PC_ln_lag1 = lag.new(expenditure.PC_ln,n=1,along_with=Year4),
      expenditure.PC_ln_lead1 = lead.new(expenditure.PC_ln,n=1,along_with=Year4),
      expenditure.PC_ln_lead2 = lead.new(expenditure.PC_ln,n=2,along_with=Year4),
      expenditure.PC_ln_lead3 = lead.new(expenditure.PC_ln,n=3,along_with=Year4),
      expenditure.PC_ln_lead4 = lead.new(expenditure.PC_ln,n=4,along_with=Year4),
      expenditure.PC_ln_lead5 = lead.new(expenditure.PC_ln,n=5,along_with=Year4),
      expenditure.PC_ln_lead6 = lead.new(expenditure.PC_ln,n=6,along_with=Year4),
      expenditure.PC_ln_deltalag3 = expenditure.PC_ln - expenditure.PC_ln_lag3,
      expenditure.PC_ln_deltalag2 = expenditure.PC_ln - expenditure.PC_ln_lag2,
      expenditure.PC_ln_deltalag1 = expenditure.PC_ln - expenditure.PC_ln_lag1,
      expenditure.PC_ln_delta1 = expenditure.PC_ln_lead1 - expenditure.PC_ln,
      expenditure.PC_ln_delta2 = expenditure.PC_ln_lead2 - expenditure.PC_ln,
      expenditure.PC_ln_delta3 = expenditure.PC_ln_lead3 - expenditure.PC_ln,
      expenditure.PC_ln_delta4 = expenditure.PC_ln_lead4 - expenditure.PC_ln,
      expenditure.PC_ln_delta5 = expenditure.PC_ln_lead5 - expenditure.PC_ln,
      expenditure.PC_ln_delta6 = expenditure.PC_ln_lead6 - expenditure.PC_ln,
      
      expenditures.PC.nohospitals_ln = log1p(expenditures.PC.nohospitals),
      expenditures.PC.nohospitals_lag3 = lag.new(expenditures.PC.nohospitals,n=3,along_with=Year4),
      expenditures.PC.nohospitals_lag2 = lag.new(expenditures.PC.nohospitals,n=2,along_with=Year4),
      expenditures.PC.nohospitals_lag1 = lag.new(expenditures.PC.nohospitals,n=1,along_with=Year4),
      expenditures.PC.nohospitals_lead1 = lead.new(expenditures.PC.nohospitals,n=1,along_with=Year4),
      expenditures.PC.nohospitals_lead2 = lead.new(expenditures.PC.nohospitals,n=2,along_with=Year4),
      expenditures.PC.nohospitals_lead3 = lead.new(expenditures.PC.nohospitals,n=3,along_with=Year4),
      expenditures.PC.nohospitals_lead4 = lead.new(expenditures.PC.nohospitals,n=4,along_with=Year4),
      expenditures.PC.nohospitals_lead5 = lead.new(expenditures.PC.nohospitals,n=5,along_with=Year4),
      expenditures.PC.nohospitals_delta1 = expenditures.PC.nohospitals_lead1 - expenditures.PC.nohospitals,
      expenditures.PC.nohospitals_delta2 = expenditures.PC.nohospitals_lead2 - expenditures.PC.nohospitals,
      expenditures.PC.nohospitals_delta3 = expenditures.PC.nohospitals_lead3 - expenditures.PC.nohospitals,
      
      expenditures.PC.nohospitals_ln_lag3 = lag.new(expenditures.PC.nohospitals_ln,n=3,along_with=Year4),
      expenditures.PC.nohospitals_ln_lag2 = lag.new(expenditures.PC.nohospitals_ln,n=2,along_with=Year4),
      expenditures.PC.nohospitals_ln_lag1 = lag.new(expenditures.PC.nohospitals_ln,n=1,along_with=Year4),
      expenditures.PC.nohospitals_ln_lead1 = lead.new(expenditures.PC.nohospitals_ln,n=1,along_with=Year4),
      expenditures.PC.nohospitals_ln_lead2 = lead.new(expenditures.PC.nohospitals_ln,n=2,along_with=Year4),
      expenditures.PC.nohospitals_ln_lead3 = lead.new(expenditures.PC.nohospitals_ln,n=3,along_with=Year4),
      expenditures.PC.nohospitals_ln_lead4 = lead.new(expenditures.PC.nohospitals_ln,n=4,along_with=Year4),
      expenditures.PC.nohospitals_ln_lead5 = lead.new(expenditures.PC.nohospitals_ln,n=5,along_with=Year4),
      expenditures.PC.nohospitals_ln_delta1 = expenditures.PC.nohospitals_ln_lead1 - expenditures.PC.nohospitals_ln,
      expenditures.PC.nohospitals_ln_delta2 = expenditures.PC.nohospitals_ln_lead2 - expenditures.PC.nohospitals_ln,
      expenditures.PC.nohospitals_ln_delta3 = expenditures.PC.nohospitals_ln_lead3 - expenditures.PC.nohospitals_ln,
      
      hospitals.PC_ln = log1p(hospitals.PC),
      hospitals.PC_lead1 = lead.new(hospitals.PC,n=1,along_with=Year4),
      hospitals.PC_lead2 = lead.new(hospitals.PC,n=2,along_with=Year4),
      hospitals.PC_lead3 = lead.new(hospitals.PC,n=3,along_with=Year4),
      hospitals.PC_delta1 = hospitals.PC_lead1 - hospitals.PC,
      hospitals.PC_delta2 = hospitals.PC_lead2 - hospitals.PC,
      hospitals.PC_delta3 = hospitals.PC_lead3 - hospitals.PC,
      
      hospitals.PC_ln_lead1 = lead.new(hospitals.PC_ln,n=1,along_with=Year4),
      hospitals.PC_ln_lead2 = lead.new(hospitals.PC_ln,n=2,along_with=Year4),
      hospitals.PC_ln_lead3 = lead.new(hospitals.PC_ln,n=3,along_with=Year4),
      hospitals.PC_ln_delta1 = hospitals.PC_ln_lead1 - hospitals.PC_ln,
      hospitals.PC_ln_delta2 = hospitals.PC_ln_lead2 - hospitals.PC_ln,
      hospitals.PC_ln_delta3 = hospitals.PC_ln_lead3 - hospitals.PC_ln,
      
      corrections.PC_ln = log1p(corrections.PC),
      corrections.PC_lead1 = lead.new(corrections.PC,n=1,along_with=Year4),
      corrections.PC_lead2 = lead.new(corrections.PC,n=2,along_with=Year4),
      corrections.PC_lead3 = lead.new(corrections.PC,n=3,along_with=Year4),
      corrections.PC_delta1 = corrections.PC_lead1 - corrections.PC,
      corrections.PC_delta2 = corrections.PC_lead2 - corrections.PC,
      corrections.PC_delta3 = corrections.PC_lead3 - corrections.PC,
      
      corrections.PC_ln_lead1 = lead.new(corrections.PC_ln,n=1,along_with=Year4),
      corrections.PC_ln_lead2 = lead.new(corrections.PC_ln,n=2,along_with=Year4),
      corrections.PC_ln_lead3 = lead.new(corrections.PC_ln,n=3,along_with=Year4),
      corrections.PC_ln_delta1 = corrections.PC_ln_lead1 - corrections.PC_ln,
      corrections.PC_ln_delta2 = corrections.PC_ln_lead2 - corrections.PC_ln,
      corrections.PC_ln_delta3 = corrections.PC_ln_lead3 - corrections.PC_ln,
      
      naturalresources.PC_ln = log1p(naturalresources.PC),
      naturalresources.PC_lead1 = lead.new(naturalresources.PC,n=1,along_with=Year4),
      naturalresources.PC_lead2 = lead.new(naturalresources.PC,n=2,along_with=Year4),
      naturalresources.PC_lead3 = lead.new(naturalresources.PC,n=3,along_with=Year4),
      naturalresources.PC_delta1 = naturalresources.PC_lead1 - naturalresources.PC,
      naturalresources.PC_delta2 = naturalresources.PC_lead2 - naturalresources.PC,
      naturalresources.PC_delta3 = naturalresources.PC_lead3 - naturalresources.PC,
      
      naturalresources.PC_ln_lead1 = lead.new(naturalresources.PC_ln,n=1,along_with=Year4),
      naturalresources.PC_ln_lead2 = lead.new(naturalresources.PC_ln,n=2,along_with=Year4),
      naturalresources.PC_ln_lead3 = lead.new(naturalresources.PC_ln,n=3,along_with=Year4),
      naturalresources.PC_ln_delta1 = naturalresources.PC_ln_lead1 - naturalresources.PC_ln,
      naturalresources.PC_ln_delta2 = naturalresources.PC_ln_lead2 - naturalresources.PC_ln,
      naturalresources.PC_ln_delta3 = naturalresources.PC_ln_lead3 - naturalresources.PC_ln,
      
      education.PC_ln = log1p(education.PC),
      education.PC_lead1 = lead.new(education.PC,n=1,along_with=Year4),
      education.PC_lead2 = lead.new(education.PC,n=2,along_with=Year4),
      education.PC_lead3 = lead.new(education.PC,n=3,along_with=Year4),
      education.PC_delta1 = education.PC_lead1 - education.PC,
      education.PC_delta2 = education.PC_lead2 - education.PC,
      education.PC_delta3 = education.PC_lead3 - education.PC,
      
      education.PC_ln_lead1 = lead.new(education.PC_ln,n=1,along_with=Year4),
      education.PC_ln_lead2 = lead.new(education.PC_ln,n=2,along_with=Year4),
      education.PC_ln_lead3 = lead.new(education.PC_ln,n=3,along_with=Year4),
      education.PC_ln_delta1 = education.PC_ln_lead1 - education.PC_ln,
      education.PC_ln_delta2 = education.PC_ln_lead2 - education.PC_ln,
      education.PC_ln_delta3 = education.PC_ln_lead3 - education.PC_ln,
      
      
      nec.PC_ln = log1p(nec.PC),
      nec.PC_lead1 = lead.new(nec.PC,n=1,along_with=Year4),
      nec.PC_lead2 = lead.new(nec.PC,n=2,along_with=Year4),
      nec.PC_lead3 = lead.new(nec.PC,n=3,along_with=Year4),
      nec.PC_delta1 = nec.PC_lead1 - nec.PC,
      nec.PC_delta2 = nec.PC_lead2 - nec.PC,
      nec.PC_delta3 = nec.PC_lead3 - nec.PC,
      
      nec.PC_ln_lead1 = lead.new(nec.PC_ln,n=1,along_with=Year4),
      nec.PC_ln_lead2 = lead.new(nec.PC_ln,n=2,along_with=Year4),
      nec.PC_ln_lead3 = lead.new(nec.PC_ln,n=3,along_with=Year4),
      nec.PC_ln_delta1 = nec.PC_ln_lead1 - nec.PC_ln,
      nec.PC_ln_delta2 = nec.PC_ln_lead2 - nec.PC_ln,
      nec.PC_ln_delta3 = nec.PC_ln_lead3 - nec.PC_ln,
      
      fire.PC_ln = log1p(fire.PC),
      fire.PC_lead1 = lead.new(fire.PC,n=1,along_with=Year4),
      fire.PC_lead2 = lead.new(fire.PC,n=2,along_with=Year4),
      fire.PC_lead3 = lead.new(fire.PC,n=3,along_with=Year4),
      fire.PC_delta1 = fire.PC_lead1 - fire.PC,
      fire.PC_delta2 = fire.PC_lead2 - fire.PC,
      fire.PC_delta3 = fire.PC_lead3 - fire.PC,
      
      fire.PC_ln_lead1 = lead.new(fire.PC_ln,n=1,along_with=Year4),
      fire.PC_ln_lead2 = lead.new(fire.PC_ln,n=2,along_with=Year4),
      fire.PC_ln_lead3 = lead.new(fire.PC_ln,n=3,along_with=Year4),
      fire.PC_ln_delta1 = fire.PC_ln_lead1 - fire.PC_ln,
      fire.PC_ln_delta2 = fire.PC_ln_lead2 - fire.PC_ln,
      fire.PC_ln_delta3 = fire.PC_ln_lead3 - fire.PC_ln,
      
      police.PC_ln = log1p(police.PC),
      police.PC_lead1 = lead.new(police.PC,n=1,along_with=Year4),
      police.PC_lead2 = lead.new(police.PC,n=2,along_with=Year4),
      police.PC_lead3 = lead.new(police.PC,n=3,along_with=Year4),
      police.PC_delta1 = police.PC_lead1 - police.PC,
      police.PC_delta2 = police.PC_lead2 - police.PC,
      police.PC_delta3 = police.PC_lead3 - police.PC,
      
      police.PC_ln_lead1 = lead.new(police.PC_ln,n=1,along_with=Year4),
      police.PC_ln_lead2 = lead.new(police.PC_ln,n=2,along_with=Year4),
      police.PC_ln_lead3 = lead.new(police.PC_ln,n=3,along_with=Year4),
      police.PC_ln_delta1 = police.PC_ln_lead1 - police.PC_ln,
      police.PC_ln_delta2 = police.PC_ln_lead2 - police.PC_ln,
      police.PC_ln_delta3 = police.PC_ln_lead3 - police.PC_ln,
      
      
      health.PC_ln = log1p(health.PC),
      health.PC_lead1 = lead.new(health.PC,n=1,along_with=Year4),
      health.PC_lead2 = lead.new(health.PC,n=2,along_with=Year4),
      health.PC_lead3 = lead.new(health.PC,n=3,along_with=Year4),
      health.PC_delta1 = health.PC_lead1 - health.PC,
      health.PC_delta2 = health.PC_lead2 - health.PC,
      health.PC_delta3 = health.PC_lead3 - health.PC,
      
      health.PC_ln_lead1 = lead.new(health.PC_ln,n=1,along_with=Year4),
      health.PC_ln_lead2 = lead.new(health.PC_ln,n=2,along_with=Year4),
      health.PC_ln_lead3 = lead.new(health.PC_ln,n=3,along_with=Year4),
      health.PC_ln_delta1 = health.PC_ln_lead1 - health.PC_ln,
      health.PC_ln_delta2 = health.PC_ln_lead2 - health.PC_ln,
      health.PC_ln_delta3 = health.PC_ln_lead3 - health.PC_ln,
      
      
      highways.PC_ln = log1p(highways.PC),
      highways.PC_lead1 = lead.new(highways.PC,n=1,along_with=Year4),
      highways.PC_lead2 = lead.new(highways.PC,n=2,along_with=Year4),
      highways.PC_lead3 = lead.new(highways.PC,n=3,along_with=Year4),
      highways.PC_delta1 = highways.PC_lead1 - highways.PC,
      highways.PC_delta2 = highways.PC_lead2 - highways.PC,
      highways.PC_delta3 = highways.PC_lead3 - highways.PC,
      
      highways.PC_ln_lead1 = lead.new(highways.PC_ln,n=1,along_with=Year4),
      highways.PC_ln_lead2 = lead.new(highways.PC_ln,n=2,along_with=Year4),
      highways.PC_ln_lead3 = lead.new(highways.PC_ln,n=3,along_with=Year4),
      highways.PC_ln_delta1 = highways.PC_ln_lead1 - highways.PC_ln,
      highways.PC_ln_delta2 = highways.PC_ln_lead2 - highways.PC_ln,
      highways.PC_ln_delta3 = highways.PC_ln_lead3 - highways.PC_ln,
      
      
      parking.PC_ln = log1p(parking.PC),
      parking.PC_lead1 = lead.new(parking.PC,n=1,along_with=Year4),
      parking.PC_lead2 = lead.new(parking.PC,n=2,along_with=Year4),
      parking.PC_lead3 = lead.new(parking.PC,n=3,along_with=Year4),
      parking.PC_delta1 = parking.PC_lead1 - parking.PC,
      parking.PC_delta2 = parking.PC_lead2 - parking.PC,
      parking.PC_delta3 = parking.PC_lead3 - parking.PC,
      
      parking.PC_ln_lead1 = lead.new(parking.PC_ln,n=1,along_with=Year4),
      parking.PC_ln_lead2 = lead.new(parking.PC_ln,n=2,along_with=Year4),
      parking.PC_ln_lead3 = lead.new(parking.PC_ln,n=3,along_with=Year4),
      parking.PC_ln_delta1 = parking.PC_ln_lead1 - parking.PC_ln,
      parking.PC_ln_delta2 = parking.PC_ln_lead2 - parking.PC_ln,
      parking.PC_ln_delta3 = parking.PC_ln_lead3 - parking.PC_ln,
      
      
      housing.PC_ln = log1p(housing.PC),
      housing.PC_lead1 = lead.new(housing.PC,n=1,along_with=Year4),
      housing.PC_lead2 = lead.new(housing.PC,n=2,along_with=Year4),
      housing.PC_lead3 = lead.new(housing.PC,n=3,along_with=Year4),
      housing.PC_delta1 = housing.PC_lead1 - housing.PC,
      housing.PC_delta2 = housing.PC_lead2 - housing.PC,
      housing.PC_delta3 = housing.PC_lead3 - housing.PC,
      
      housing.PC_ln_lead1 = lead.new(housing.PC_ln,n=1,along_with=Year4),
      housing.PC_ln_lead2 = lead.new(housing.PC_ln,n=2,along_with=Year4),
      housing.PC_ln_lead3 = lead.new(housing.PC_ln,n=3,along_with=Year4),
      housing.PC_ln_delta1 = housing.PC_ln_lead1 - housing.PC_ln,
      housing.PC_ln_delta2 = housing.PC_ln_lead2 - housing.PC_ln,
      housing.PC_ln_delta3 = housing.PC_ln_lead3 - housing.PC_ln,
      
      
      libraries.PC_ln = log1p(libraries.PC),
      libraries.PC_lead1 = lead.new(libraries.PC,n=1,along_with=Year4),
      libraries.PC_lead2 = lead.new(libraries.PC,n=2,along_with=Year4),
      libraries.PC_lead3 = lead.new(libraries.PC,n=3,along_with=Year4),
      libraries.PC_delta1 = libraries.PC_lead1 - libraries.PC,
      libraries.PC_delta2 = libraries.PC_lead2 - libraries.PC,
      libraries.PC_delta3 = libraries.PC_lead3 - libraries.PC,
      
      libraries.PC_ln_lead1 = lead.new(libraries.PC_ln,n=1,along_with=Year4),
      libraries.PC_ln_lead2 = lead.new(libraries.PC_ln,n=2,along_with=Year4),
      libraries.PC_ln_lead3 = lead.new(libraries.PC_ln,n=3,along_with=Year4),
      libraries.PC_ln_delta1 = libraries.PC_ln_lead1 - libraries.PC_ln,
      libraries.PC_ln_delta2 = libraries.PC_ln_lead2 - libraries.PC_ln,
      libraries.PC_ln_delta3 = libraries.PC_ln_lead3 - libraries.PC_ln,
      
      
      parks.PC_ln = log1p(parks.PC),
      parks.PC_lead1 = lead.new(parks.PC,n=1,along_with=Year4),
      parks.PC_lead2 = lead.new(parks.PC,n=2,along_with=Year4),
      parks.PC_lead3 = lead.new(parks.PC,n=3,along_with=Year4),
      parks.PC_delta1 = parks.PC_lead1 - parks.PC,
      parks.PC_delta2 = parks.PC_lead2 - parks.PC,
      parks.PC_delta3 = parks.PC_lead3 - parks.PC,
      
      parks.PC_ln_lead1 = lead.new(parks.PC_ln,n=1,along_with=Year4),
      parks.PC_ln_lead2 = lead.new(parks.PC_ln,n=2,along_with=Year4),
      parks.PC_ln_lead3 = lead.new(parks.PC_ln,n=3,along_with=Year4),
      parks.PC_ln_delta1 = parks.PC_ln_lead1 - parks.PC_ln,
      parks.PC_ln_delta2 = parks.PC_ln_lead2 - parks.PC_ln,
      parks.PC_ln_delta3 = parks.PC_ln_lead3 - parks.PC_ln,
      
      
      sanitation.PC_ln = log1p(sanitation.PC),
      sanitation.PC_lead1 = lead.new(sanitation.PC,n=1,along_with=Year4),
      sanitation.PC_lead2 = lead.new(sanitation.PC,n=2,along_with=Year4),
      sanitation.PC_lead3 = lead.new(sanitation.PC,n=3,along_with=Year4),
      sanitation.PC_delta1 = sanitation.PC_lead1 - sanitation.PC,
      sanitation.PC_delta2 = sanitation.PC_lead2 - sanitation.PC,
      sanitation.PC_delta3 = sanitation.PC_lead3 - sanitation.PC,
      
      sanitation.PC_ln_lead1 = lead.new(sanitation.PC_ln,n=1,along_with=Year4),
      sanitation.PC_ln_lead2 = lead.new(sanitation.PC_ln,n=2,along_with=Year4),
      sanitation.PC_ln_lead3 = lead.new(sanitation.PC_ln,n=3,along_with=Year4),
      sanitation.PC_ln_delta1 = sanitation.PC_ln_lead1 - sanitation.PC_ln,
      sanitation.PC_ln_delta2 = sanitation.PC_ln_lead2 - sanitation.PC_ln,
      sanitation.PC_ln_delta3 = sanitation.PC_ln_lead3 - sanitation.PC_ln,
      
      
      utilities.PC_ln = log1p(utilities.PC),
      utilities.PC_lead1 = lead.new(utilities.PC,n=1,along_with=Year4),
      utilities.PC_lead2 = lead.new(utilities.PC,n=2,along_with=Year4),
      utilities.PC_lead3 = lead.new(utilities.PC,n=3,along_with=Year4),
      utilities.PC_delta1 = utilities.PC_lead1 - utilities.PC,
      utilities.PC_delta2 = utilities.PC_lead2 - utilities.PC,
      utilities.PC_delta3 = utilities.PC_lead3 - utilities.PC,
      
      utilities.PC_ln_lead1 = lead.new(utilities.PC_ln,n=1,along_with=Year4),
      utilities.PC_ln_lead2 = lead.new(utilities.PC_ln,n=2,along_with=Year4),
      utilities.PC_ln_lead3 = lead.new(utilities.PC_ln,n=3,along_with=Year4),
      utilities.PC_ln_delta1 = utilities.PC_ln_lead1 - utilities.PC_ln,
      utilities.PC_ln_delta2 = utilities.PC_ln_lead2 - utilities.PC_ln,
      utilities.PC_ln_delta3 = utilities.PC_ln_lead3 - utilities.PC_ln,
      
      
      welfare.PC_ln = log1p(welfare.PC),
      welfare.PC_lead1 = lead.new(welfare.PC,n=1,along_with=Year4),
      welfare.PC_lead2 = lead.new(welfare.PC,n=2,along_with=Year4),
      welfare.PC_lead3 = lead.new(welfare.PC,n=3,along_with=Year4),
      welfare.PC_delta1 = welfare.PC_lead1 - welfare.PC,
      welfare.PC_delta2 = welfare.PC_lead2 - welfare.PC,
      welfare.PC_delta3 = welfare.PC_lead3 - welfare.PC,
      
      welfare.PC_ln_lead1 = lead.new(welfare.PC_ln,n=1,along_with=Year4),
      welfare.PC_ln_lead2 = lead.new(welfare.PC_ln,n=2,along_with=Year4),
      welfare.PC_ln_lead3 = lead.new(welfare.PC_ln,n=3,along_with=Year4),
      welfare.PC_ln_delta1 = welfare.PC_ln_lead1 - welfare.PC_ln,
      welfare.PC_ln_delta2 = welfare.PC_ln_lead2 - welfare.PC_ln,
      welfare.PC_ln_delta3 = welfare.PC_ln_lead3 - welfare.PC_ln,
      
      
      interest.PC_ln = log1p(interest.PC),
      interest.PC_lead1 = lead.new(interest.PC,n=1,along_with=Year4),
      interest.PC_lead2 = lead.new(interest.PC,n=2,along_with=Year4),
      interest.PC_lead3 = lead.new(interest.PC,n=3,along_with=Year4),
      interest.PC_delta1 = interest.PC_lead1 - interest.PC,
      interest.PC_delta2 = interest.PC_lead2 - interest.PC,
      interest.PC_delta3 = interest.PC_lead3 - interest.PC,
      
      interest.PC_ln_lead1 = lead.new(interest.PC_ln,n=1,along_with=Year4),
      interest.PC_ln_lead2 = lead.new(interest.PC_ln,n=2,along_with=Year4),
      interest.PC_ln_lead3 = lead.new(interest.PC_ln,n=3,along_with=Year4),
      interest.PC_ln_delta1 = interest.PC_ln_lead1 - interest.PC_ln,
      interest.PC_ln_delta2 = interest.PC_ln_lead2 - interest.PC_ln,
      interest.PC_ln_delta3 = interest.PC_ln_lead3 - interest.PC_ln,
      
      
      admin.PC_ln = log1p(admin.PC),
      admin.PC_lead1 = lead.new(admin.PC,n=1,along_with=Year4),
      admin.PC_lead2 = lead.new(admin.PC,n=2,along_with=Year4),
      admin.PC_lead3 = lead.new(admin.PC,n=3,along_with=Year4),
      admin.PC_delta1 = admin.PC_lead1 - admin.PC,
      admin.PC_delta2 = admin.PC_lead2 - admin.PC,
      admin.PC_delta3 = admin.PC_lead3 - admin.PC,
      
      admin.PC_ln_lead1 = lead.new(admin.PC_ln,n=1,along_with=Year4),
      admin.PC_ln_lead2 = lead.new(admin.PC_ln,n=2,along_with=Year4),
      admin.PC_ln_lead3 = lead.new(admin.PC_ln,n=3,along_with=Year4),
      admin.PC_ln_delta1 = admin.PC_ln_lead1 - admin.PC_ln,
      admin.PC_ln_delta2 = admin.PC_ln_lead2 - admin.PC_ln,
      admin.PC_ln_delta3 = admin.PC_ln_lead3 - admin.PC_ln,
      
      
      ## Revenue:
      revenue.PC_ln = log1p(revenue.PC),
      revenue.PC_lag3 = lag.new(revenue.PC,n=3,along_with=Year4),
      revenue.PC_lag2 = lag.new(revenue.PC,n=2,along_with=Year4),
      revenue.PC_lag1 = lag.new(revenue.PC,n=1,along_with=Year4),
      revenue.PC_lead1 = lead.new(revenue.PC,n=1,along_with=Year4),
      revenue.PC_lead2 = lead.new(revenue.PC,n=2,along_with=Year4),
      revenue.PC_lead3 = lead.new(revenue.PC,n=3,along_with=Year4),
      revenue.PC_delta1 = revenue.PC_lead1 - revenue.PC,
      revenue.PC_delta2 = revenue.PC_lead2 - revenue.PC,
      revenue.PC_delta3 = revenue.PC_lead3 - revenue.PC,
      
      revenue.PC_ln_lag3 = lag.new(revenue.PC_ln,n=3,along_with=Year4),
      revenue.PC_ln_lag2 = lag.new(revenue.PC_ln,n=2,along_with=Year4),
      revenue.PC_ln_lag1 = lag.new(revenue.PC_ln,n=1,along_with=Year4),
      revenue.PC_ln_lead1 = lead.new(revenue.PC_ln,n=1,along_with=Year4),
      revenue.PC_ln_lead2 = lead.new(revenue.PC_ln,n=2,along_with=Year4),
      revenue.PC_ln_lead3 = lead.new(revenue.PC_ln,n=3,along_with=Year4),
      revenue.PC_ln_lead4 = lead.new(revenue.PC_ln,n=4,along_with=Year4),
      revenue.PC_ln_lead5 = lead.new(revenue.PC_ln,n=5,along_with=Year4),
      revenue.PC_ln_lead6 = lead.new(revenue.PC_ln,n=6,along_with=Year4),
      revenue.PC_ln_delta1 = revenue.PC_ln_lead1 - revenue.PC_ln,
      revenue.PC_ln_delta2 = revenue.PC_ln_lead2 - revenue.PC_ln,
      revenue.PC_ln_delta3 = revenue.PC_ln_lead3 - revenue.PC_ln,
      revenue.PC_ln_delta4 = revenue.PC_ln_lead4 - revenue.PC_ln,
      revenue.PC_ln_delta5 = revenue.PC_ln_lead5 - revenue.PC_ln,
      revenue.PC_ln_delta6 = revenue.PC_ln_lead6 - revenue.PC_ln,
      
      
      revenueown.PC_ln = log1p(revenueown.PC),
      revenueown.PC_lead1 = lead.new(revenueown.PC,n=1,along_with=Year4),
      revenueown.PC_lead2 = lead.new(revenueown.PC,n=2,along_with=Year4),
      revenueown.PC_lead3 = lead.new(revenueown.PC,n=3,along_with=Year4),
      revenueown.PC_delta1 = revenueown.PC_lead1 - revenueown.PC,
      revenueown.PC_delta2 = revenueown.PC_lead2 - revenueown.PC,
      revenueown.PC_delta3 = revenueown.PC_lead3 - revenueown.PC,
      
      revenueown.PC_ln_lead1 = lead.new(revenueown.PC_ln,n=1,along_with=Year4),
      revenueown.PC_ln_lead2 = lead.new(revenueown.PC_ln,n=2,along_with=Year4),
      revenueown.PC_ln_lead3 = lead.new(revenueown.PC_ln,n=3,along_with=Year4),
      revenueown.PC_ln_delta1 = revenueown.PC_ln_lead1 - revenueown.PC_ln,
      revenueown.PC_ln_delta2 = revenueown.PC_ln_lead2 - revenueown.PC_ln,
      revenueown.PC_ln_delta3 = revenueown.PC_ln_lead3 - revenueown.PC_ln,
      
      
      revenuegeneral.PC_ln = log1p(revenuegeneral.PC),
      revenuegeneral.PC_lead1 = lead.new(revenuegeneral.PC,n=1,along_with=Year4),
      revenuegeneral.PC_lead2 = lead.new(revenuegeneral.PC,n=2,along_with=Year4),
      revenuegeneral.PC_lead3 = lead.new(revenuegeneral.PC,n=3,along_with=Year4),
      revenuegeneral.PC_delta1 = revenuegeneral.PC_lead1 - revenuegeneral.PC,
      revenuegeneral.PC_delta2 = revenuegeneral.PC_lead2 - revenuegeneral.PC,
      revenuegeneral.PC_delta3 = revenuegeneral.PC_lead3 - revenuegeneral.PC,
      
      revenuegeneral.PC_ln_lead1 = lead.new(revenuegeneral.PC_ln,n=1,along_with=Year4),
      revenuegeneral.PC_ln_lead2 = lead.new(revenuegeneral.PC_ln,n=2,along_with=Year4),
      revenuegeneral.PC_ln_lead3 = lead.new(revenuegeneral.PC_ln,n=3,along_with=Year4),
      revenuegeneral.PC_ln_delta1 = revenuegeneral.PC_ln_lead1 - revenuegeneral.PC_ln,
      revenuegeneral.PC_ln_delta2 = revenuegeneral.PC_ln_lead2 - revenuegeneral.PC_ln,
      revenuegeneral.PC_ln_delta3 = revenuegeneral.PC_ln_lead3 - revenuegeneral.PC_ln,
      
      
      revenueowngeneral.PC_ln = log1p(revenueowngeneral.PC),
      revenueowngeneral.PC_lead1 = lead.new(revenueowngeneral.PC,n=1,along_with=Year4),
      revenueowngeneral.PC_lead2 = lead.new(revenueowngeneral.PC,n=2,along_with=Year4),
      revenueowngeneral.PC_lead3 = lead.new(revenueowngeneral.PC,n=3,along_with=Year4),
      revenueowngeneral.PC_delta1 = revenueowngeneral.PC_lead1 - revenueowngeneral.PC,
      revenueowngeneral.PC_delta2 = revenueowngeneral.PC_lead2 - revenueowngeneral.PC,
      revenueowngeneral.PC_delta3 = revenueowngeneral.PC_lead3 - revenueowngeneral.PC,
      
      revenueowngeneral.PC_ln_lead1 = lead.new(revenueowngeneral.PC_ln,n=1,along_with=Year4),
      revenueowngeneral.PC_ln_lead2 = lead.new(revenueowngeneral.PC_ln,n=2,along_with=Year4),
      revenueowngeneral.PC_ln_lead3 = lead.new(revenueowngeneral.PC_ln,n=3,along_with=Year4),
      revenueowngeneral.PC_ln_delta1 = revenueowngeneral.PC_ln_lead1 - revenueowngeneral.PC_ln,
      revenueowngeneral.PC_ln_delta2 = revenueowngeneral.PC_ln_lead2 - revenueowngeneral.PC_ln,
      revenueowngeneral.PC_ln_delta3 = revenueowngeneral.PC_ln_lead3 - revenueowngeneral.PC_ln,
      
      
      taxes.PC_ln = log1p(Total.Taxes.PC),
      taxes.PC_lead1 = lead.new(Total.Taxes.PC,n=1,along_with=Year4),
      taxes.PC_lead2 = lead.new(Total.Taxes.PC,n=2,along_with=Year4),
      taxes.PC_lead3 = lead.new(Total.Taxes.PC,n=3,along_with=Year4),
      taxes.PC_delta1 = taxes.PC_lead1 - Total.Taxes.PC,
      taxes.PC_delta2 = taxes.PC_lead2 - Total.Taxes.PC,
      taxes.PC_delta3 = taxes.PC_lead3 - Total.Taxes.PC,
      
      taxes.PC_ln_lead1 = lead.new(taxes.PC_ln,n=1,along_with=Year4),
      taxes.PC_ln_lead2 = lead.new(taxes.PC_ln,n=2,along_with=Year4),
      taxes.PC_ln_lead3 = lead.new(taxes.PC_ln,n=3,along_with=Year4),
      taxes.PC_ln_delta1 = taxes.PC_ln_lead1 - taxes.PC_ln,
      taxes.PC_ln_delta2 = taxes.PC_ln_lead2 - taxes.PC_ln,
      taxes.PC_ln_delta3 = taxes.PC_ln_lead3 - taxes.PC_ln,
      
      
      propertytax.PC_ln = log1p(propertytax.PC),
      propertytax.PC_lead1 = lead.new(propertytax.PC,n=1,along_with=Year4),
      propertytax.PC_lead2 = lead.new(propertytax.PC,n=2,along_with=Year4),
      propertytax.PC_lead3 = lead.new(propertytax.PC,n=3,along_with=Year4),
      propertytax.PC_delta1 = propertytax.PC_lead1 - propertytax.PC,
      propertytax.PC_delta2 = propertytax.PC_lead2 - propertytax.PC,
      propertytax.PC_delta3 = propertytax.PC_lead3 - propertytax.PC,
      
      propertytax.PC_ln_lead1 = lead.new(propertytax.PC_ln,n=1,along_with=Year4),
      propertytax.PC_ln_lead2 = lead.new(propertytax.PC_ln,n=2,along_with=Year4),
      propertytax.PC_ln_lead3 = lead.new(propertytax.PC_ln,n=3,along_with=Year4),
      propertytax.PC_ln_delta1 = propertytax.PC_ln_lead1 - propertytax.PC_ln,
      propertytax.PC_ln_delta2 = propertytax.PC_ln_lead2 - propertytax.PC_ln,
      propertytax.PC_ln_delta3 = propertytax.PC_ln_lead3 - propertytax.PC_ln,
      
      
      salestax.select.PC_ln = log1p(salestax.select.PC),
      salestax.select.PC_lead1 = lead.new(salestax.select.PC,n=1,along_with=Year4),
      salestax.select.PC_lead2 = lead.new(salestax.select.PC,n=2,along_with=Year4),
      salestax.select.PC_lead3 = lead.new(salestax.select.PC,n=3,along_with=Year4),
      salestax.select.PC_delta1 = salestax.select.PC_lead1 - salestax.select.PC,
      salestax.select.PC_delta2 = salestax.select.PC_lead2 - salestax.select.PC,
      salestax.select.PC_delta3 = salestax.select.PC_lead3 - salestax.select.PC,
      
      salestax.select.PC_ln_lead1 = lead.new(salestax.select.PC_ln,n=1,along_with=Year4),
      salestax.select.PC_ln_lead2 = lead.new(salestax.select.PC_ln,n=2,along_with=Year4),
      salestax.select.PC_ln_lead3 = lead.new(salestax.select.PC_ln,n=3,along_with=Year4),
      salestax.select.PC_ln_delta1 = salestax.select.PC_ln_lead1 - salestax.select.PC_ln,
      salestax.select.PC_ln_delta2 = salestax.select.PC_ln_lead2 - salestax.select.PC_ln,
      salestax.select.PC_ln_delta3 = salestax.select.PC_ln_lead3 - salestax.select.PC_ln,
      
      
      salestax.general.PC_ln = log1p(salestax.general.PC),
      salestax.general.PC_lead1 = lead.new(salestax.general.PC,n=1,along_with=Year4),
      salestax.general.PC_lead2 = lead.new(salestax.general.PC,n=2,along_with=Year4),
      salestax.general.PC_lead3 = lead.new(salestax.general.PC,n=3,along_with=Year4),
      salestax.general.PC_delta1 = salestax.general.PC_lead1 - salestax.general.PC,
      salestax.general.PC_delta2 = salestax.general.PC_lead2 - salestax.general.PC,
      salestax.general.PC_delta3 = salestax.general.PC_lead3 - salestax.general.PC,
      
      salestax.general.PC_ln_lead1 = lead.new(salestax.general.PC_ln,n=1,along_with=Year4),
      salestax.general.PC_ln_lead2 = lead.new(salestax.general.PC_ln,n=2,along_with=Year4),
      salestax.general.PC_ln_lead3 = lead.new(salestax.general.PC_ln,n=3,along_with=Year4),
      salestax.general.PC_ln_delta1 = salestax.general.PC_ln_lead1 - salestax.general.PC_ln,
      salestax.general.PC_ln_delta2 = salestax.general.PC_ln_lead2 - salestax.general.PC_ln,
      salestax.general.PC_ln_delta3 = salestax.general.PC_ln_lead3 - salestax.general.PC_ln,
      
      
      igrev.state.PC_ln = log1p(igrev.state.PC),
      igrev.state.PC_lead1 = lead.new(igrev.state.PC,n=1,along_with=Year4),
      igrev.state.PC_lead2 = lead.new(igrev.state.PC,n=2,along_with=Year4),
      igrev.state.PC_lead3 = lead.new(igrev.state.PC,n=3,along_with=Year4),
      igrev.state.PC_delta1 = igrev.state.PC_lead1 - igrev.state.PC,
      igrev.state.PC_delta2 = igrev.state.PC_lead2 - igrev.state.PC_lead1,
      igrev.state.PC_delta3 = igrev.state.PC_lead3 - igrev.state.PC_lead2,
      
      igrev.state.PC_ln_lead1 = lead.new(igrev.state.PC_ln,n=1,along_with=Year4),
      igrev.state.PC_ln_lead2 = lead.new(igrev.state.PC_ln,n=2,along_with=Year4),
      igrev.state.PC_ln_lead3 = lead.new(igrev.state.PC_ln,n=3,along_with=Year4),
      igrev.state.PC_ln_delta1 = igrev.state.PC_ln_lead1 - igrev.state.PC_ln,
      igrev.state.PC_ln_delta2 = igrev.state.PC_ln_lead2 - igrev.state.PC_ln_lead1,
      igrev.state.PC_ln_delta3 = igrev.state.PC_ln_lead3 - igrev.state.PC_ln_lead2,
      
      
      igrev.PC_ln = log1p(igrev.PC),
      igrev.PC_lead1 = lead.new(igrev.PC,n=1,along_with=Year4),
      igrev.PC_lead2 = lead.new(igrev.PC,n=2,along_with=Year4),
      igrev.PC_lead3 = lead.new(igrev.PC,n=3,along_with=Year4),
      igrev.PC_delta1 = igrev.PC_lead1 - igrev.PC,
      igrev.PC_delta2 = igrev.PC_lead2 - igrev.PC_lead1,
      igrev.PC_delta3 = igrev.PC_lead3 - igrev.PC_lead2,
      
      igrev.PC_ln_lead1 = lead.new(igrev.PC_ln,n=1,along_with=Year4),
      igrev.PC_ln_lead2 = lead.new(igrev.PC_ln,n=2,along_with=Year4),
      igrev.PC_ln_lead3 = lead.new(igrev.PC_ln,n=3,along_with=Year4),
      igrev.PC_ln_delta1 = igrev.PC_ln_lead1 - igrev.PC_ln,
      igrev.PC_ln_delta2 = igrev.PC_ln_lead2 - igrev.PC_ln_lead1,
      igrev.PC_ln_delta3 = igrev.PC_ln_lead3 - igrev.PC_ln_lead2,
      
      
      charges.PC_ln = log1p(charges.PC),
      charges.PC_lead1 = lead.new(charges.PC,n=1,along_with=Year4),
      charges.PC_lead2 = lead.new(charges.PC,n=2,along_with=Year4),
      charges.PC_lead3 = lead.new(charges.PC,n=3,along_with=Year4),
      charges.PC_delta1 = charges.PC_lead1 - charges.PC,
      charges.PC_delta2 = charges.PC_lead2 - charges.PC,
      charges.PC_delta3 = charges.PC_lead3 - charges.PC,
      
      charges.PC_ln_lead1 = lead.new(charges.PC_ln,n=1,along_with=Year4),
      charges.PC_ln_lead2 = lead.new(charges.PC_ln,n=2,along_with=Year4),
      charges.PC_ln_lead3 = lead.new(charges.PC_ln,n=3,along_with=Year4),
      charges.PC_ln_delta1 = charges.PC_ln_lead1 - charges.PC_ln,
      charges.PC_ln_delta2 = charges.PC_ln_lead2 - charges.PC_ln,
      charges.PC_ln_delta3 = charges.PC_ln_lead3 - charges.PC_ln,
      
      
      utilitiesrev.PC_ln = log1p(utilitiesrev.PC),
      utilitiesrev.PC_lead1 = lead.new(utilitiesrev.PC,n=1,along_with=Year4),
      utilitiesrev.PC_lead2 = lead.new(utilitiesrev.PC,n=2,along_with=Year4),
      utilitiesrev.PC_lead3 = lead.new(utilitiesrev.PC,n=3,along_with=Year4),
      utilitiesrev.PC_delta1 = utilitiesrev.PC_lead1 - utilitiesrev.PC,
      utilitiesrev.PC_delta2 = utilitiesrev.PC_lead2 - utilitiesrev.PC,
      utilitiesrev.PC_delta3 = utilitiesrev.PC_lead3 - utilitiesrev.PC,
      
      utilitiesrev.PC_ln_lead1 = lead.new(utilitiesrev.PC_ln,n=1,along_with=Year4),
      utilitiesrev.PC_ln_lead2 = lead.new(utilitiesrev.PC_ln,n=2,along_with=Year4),
      utilitiesrev.PC_ln_lead3 = lead.new(utilitiesrev.PC_ln,n=3,along_with=Year4),
      utilitiesrev.PC_ln_delta1 = utilitiesrev.PC_ln_lead1 - utilitiesrev.PC_ln,
      utilitiesrev.PC_ln_delta2 = utilitiesrev.PC_ln_lead2 - utilitiesrev.PC_ln,
      utilitiesrev.PC_ln_delta3 = utilitiesrev.PC_ln_lead3 - utilitiesrev.PC_ln,
      
      
      ## Debt:
      debt.PC_ln = log1p(debt.PC),
      debt.PC_lag3 = lag.new(debt.PC,n=3,along_with=Year4),
      debt.PC_lag2 = lag.new(debt.PC,n=2,along_with=Year4),
      debt.PC_lag1 = lag.new(debt.PC,n=1,along_with=Year4),
      debt.PC_lead1 = lead.new(debt.PC,n=1,along_with=Year4),
      debt.PC_lead2 = lead.new(debt.PC,n=2,along_with=Year4),
      debt.PC_lead3 = lead.new(debt.PC,n=3,along_with=Year4),
      debt.PC_lead4 = lead.new(debt.PC,n=4,along_with=Year4),
      debt.PC_lead5 = lead.new(debt.PC,n=5,along_with=Year4),
      debt.PC_delta1 = debt.PC_lead1 - debt.PC,
      debt.PC_delta2 = debt.PC_lead2 - debt.PC,
      debt.PC_delta3 = debt.PC_lead3 - debt.PC,
      
      debt.PC_ln_lag3 = lag.new(debt.PC_ln,n=3,along_with=Year4),
      debt.PC_ln_lag2 = lag.new(debt.PC_ln,n=2,along_with=Year4),
      debt.PC_ln_lag1 = lag.new(debt.PC_ln,n=1,along_with=Year4),
      debt.PC_ln_lead1 = lead.new(debt.PC_ln,n=1,along_with=Year4),
      debt.PC_ln_lead2 = lead.new(debt.PC_ln,n=2,along_with=Year4),
      debt.PC_ln_lead3 = lead.new(debt.PC_ln,n=3,along_with=Year4),
      debt.PC_ln_lead4 = lead.new(debt.PC_ln,n=4,along_with=Year4),
      debt.PC_ln_lead5 = lead.new(debt.PC_ln,n=5,along_with=Year4),
      debt.PC_ln_lead6 = lead.new(debt.PC_ln,n=6,along_with=Year4),
      debt.PC_ln_delta1 = debt.PC_ln_lead1 - debt.PC_ln,
      debt.PC_ln_delta2 = debt.PC_ln_lead2 - debt.PC_ln,
      debt.PC_ln_delta3 = debt.PC_ln_lead3 - debt.PC_ln,
      debt.PC_ln_delta4 = debt.PC_ln_lead4 - debt.PC_ln,
      debt.PC_ln_delta5 = debt.PC_ln_lead5 - debt.PC_ln,
      debt.PC_ln_delta6 = debt.PC_ln_lead6 - debt.PC_ln,
      
      
      generaldebt.PC_ln = log1p(generaldebt.PC),
      generaldebt.PC_lead1 = lead.new(generaldebt.PC,n=1,along_with=Year4),
      generaldebt.PC_lead2 = lead.new(generaldebt.PC,n=2,along_with=Year4),
      generaldebt.PC_lead3 = lead.new(generaldebt.PC,n=3,along_with=Year4),
      generaldebt.PC_lead4 = lead.new(generaldebt.PC,n=4,along_with=Year4),
      generaldebt.PC_lead5 = lead.new(generaldebt.PC,n=5,along_with=Year4),
      generaldebt.PC_delta1 = generaldebt.PC_lead1 - generaldebt.PC,
      generaldebt.PC_delta2 = generaldebt.PC_lead2 - generaldebt.PC,
      generaldebt.PC_delta3 = generaldebt.PC_lead3 - generaldebt.PC,
      
      generaldebt.PC_ln_lead1 = lead.new(generaldebt.PC_ln,n=1,along_with=Year4),
      generaldebt.PC_ln_lead2 = lead.new(generaldebt.PC_ln,n=2,along_with=Year4),
      generaldebt.PC_ln_lead3 = lead.new(generaldebt.PC_ln,n=3,along_with=Year4),
      generaldebt.PC_ln_lead4 = lead.new(generaldebt.PC_ln,n=4,along_with=Year4),
      generaldebt.PC_ln_lead5 = lead.new(generaldebt.PC_ln,n=5,along_with=Year4),
      generaldebt.PC_ln_delta1 = generaldebt.PC_ln_lead1 - generaldebt.PC_ln,
      generaldebt.PC_ln_delta2 = generaldebt.PC_ln_lead2 - generaldebt.PC_ln,
      generaldebt.PC_ln_delta3 = generaldebt.PC_ln_lead3 - generaldebt.PC_ln,
      
      
      LT.debt.ffc.PC_ln = log1p(LT.debt.ffc.PC),
      LT.debt.ffc.PC_lead1 = lead.new(LT.debt.ffc.PC,n=1,along_with=Year4),
      LT.debt.ffc.PC_lead2 = lead.new(LT.debt.ffc.PC,n=2,along_with=Year4),
      LT.debt.ffc.PC_lead3 = lead.new(LT.debt.ffc.PC,n=3,along_with=Year4),
      LT.debt.ffc.PC_lead4 = lead.new(LT.debt.ffc.PC,n=4,along_with=Year4),
      LT.debt.ffc.PC_lead5 = lead.new(LT.debt.ffc.PC,n=5,along_with=Year4),
      LT.debt.ffc.PC_delta1 = LT.debt.ffc.PC_lead1 - LT.debt.ffc.PC,
      LT.debt.ffc.PC_delta2 = LT.debt.ffc.PC_lead2 - LT.debt.ffc.PC,
      LT.debt.ffc.PC_delta3 = LT.debt.ffc.PC_lead3 - LT.debt.ffc.PC,
      
      LT.debt.ffc.PC_ln_lead1 = lead.new(LT.debt.ffc.PC_ln,n=1,along_with=Year4),
      LT.debt.ffc.PC_ln_lead2 = lead.new(LT.debt.ffc.PC_ln,n=2,along_with=Year4),
      LT.debt.ffc.PC_ln_lead3 = lead.new(LT.debt.ffc.PC_ln,n=3,along_with=Year4),
      LT.debt.ffc.PC_ln_lead4 = lead.new(LT.debt.ffc.PC_ln,n=4,along_with=Year4),
      LT.debt.ffc.PC_ln_lead5 = lead.new(LT.debt.ffc.PC_ln,n=5,along_with=Year4),
      LT.debt.ffc.PC_ln_delta1 = LT.debt.ffc.PC_ln_lead1 - LT.debt.ffc.PC_ln,
      LT.debt.ffc.PC_ln_delta2 = LT.debt.ffc.PC_ln_lead2 - LT.debt.ffc.PC_ln,
      LT.debt.ffc.PC_ln_delta3 = LT.debt.ffc.PC_ln_lead3 - LT.debt.ffc.PC_ln,
      
      
      LT.debt.ng.PC_ln = log1p(LT.debt.ng.PC),
      LT.debt.ng.PC_lead1 = lead.new(LT.debt.ng.PC,n=1,along_with=Year4),
      LT.debt.ng.PC_lead2 = lead.new(LT.debt.ng.PC,n=2,along_with=Year4),
      LT.debt.ng.PC_lead3 = lead.new(LT.debt.ng.PC,n=3,along_with=Year4),
      LT.debt.ng.PC_lead4 = lead.new(LT.debt.ng.PC,n=4,along_with=Year4),
      LT.debt.ng.PC_lead5 = lead.new(LT.debt.ng.PC,n=5,along_with=Year4),
      LT.debt.ng.PC_delta1 = LT.debt.ng.PC_lead1 - LT.debt.ng.PC,
      LT.debt.ng.PC_delta2 = LT.debt.ng.PC_lead2 - LT.debt.ng.PC,
      LT.debt.ng.PC_delta3 = LT.debt.ng.PC_lead3 - LT.debt.ng.PC,
      
      LT.debt.ng.PC_ln_lead1 = lead.new(LT.debt.ng.PC_ln,n=1,along_with=Year4),
      LT.debt.ng.PC_ln_lead2 = lead.new(LT.debt.ng.PC_ln,n=2,along_with=Year4),
      LT.debt.ng.PC_ln_lead3 = lead.new(LT.debt.ng.PC_ln,n=3,along_with=Year4),
      LT.debt.ng.PC_ln_lead4 = lead.new(LT.debt.ng.PC_ln,n=4,along_with=Year4),
      LT.debt.ng.PC_ln_lead5 = lead.new(LT.debt.ng.PC_ln,n=5,along_with=Year4),
      LT.debt.ng.PC_ln_delta1 = LT.debt.ng.PC_ln_lead1 - LT.debt.ng.PC_ln,
      LT.debt.ng.PC_ln_delta2 = LT.debt.ng.PC_ln_lead2 - LT.debt.ng.PC_ln,
      LT.debt.ng.PC_ln_delta3 = LT.debt.ng.PC_ln_lead3 - LT.debt.ng.PC_ln,
      
      
      LT.debt.PC_ln = log1p(LT.debt.PC),
      LT.debt.PC_lead1 = lead.new(LT.debt.PC,n=1,along_with=Year4),
      LT.debt.PC_lead2 = lead.new(LT.debt.PC,n=2,along_with=Year4),
      LT.debt.PC_lead3 = lead.new(LT.debt.PC,n=3,along_with=Year4),
      LT.debt.PC_lead4 = lead.new(LT.debt.PC,n=4,along_with=Year4),
      LT.debt.PC_lead5 = lead.new(LT.debt.PC,n=5,along_with=Year4),
      LT.debt.PC_delta1 = LT.debt.PC_lead1 - LT.debt.PC,
      LT.debt.PC_delta2 = LT.debt.PC_lead2 - LT.debt.PC,
      LT.debt.PC_delta3 = LT.debt.PC_lead3 - LT.debt.PC,
      
      LT.debt.PC_ln_lead1 = lead.new(LT.debt.PC_ln,n=1,along_with=Year4),
      LT.debt.PC_ln_lead2 = lead.new(LT.debt.PC_ln,n=2,along_with=Year4),
      LT.debt.PC_ln_lead3 = lead.new(LT.debt.PC_ln,n=3,along_with=Year4),
      LT.debt.PC_ln_lead4 = lead.new(LT.debt.PC_ln,n=4,along_with=Year4),
      LT.debt.PC_ln_lead5 = lead.new(LT.debt.PC_ln,n=5,along_with=Year4),
      LT.debt.PC_ln_delta1 = LT.debt.PC_ln_lead1 - LT.debt.PC_ln,
      LT.debt.PC_ln_delta2 = LT.debt.PC_ln_lead2 - LT.debt.PC_ln,
      LT.debt.PC_ln_delta3 = LT.debt.PC_ln_lead3 - LT.debt.PC_ln
    )
  
  ## add 2/3 year avg variables
  IndFin_formerge <- IndFin_formerge %>%
    rowwise() %>%
    mutate(
      expenditure.PC_ln_delta23avg = mean(c(expenditure.PC_ln_delta2,expenditure.PC_ln_delta3),na.rm=T),
      hospitals.PC_ln_delta23avg = mean(c(hospitals.PC_ln_delta2,hospitals.PC_ln_delta3),na.rm=T),
      corrections.PC_ln_delta23avg = mean(c(corrections.PC_ln_delta2,corrections.PC_ln_delta3),na.rm=T),
      naturalresources.PC_ln_delta23avg = mean(c(naturalresources.PC_ln_delta2,naturalresources.PC_ln_delta3),na.rm=T),
      education.PC_ln_delta23avg = mean(c(education.PC_ln_delta2,education.PC_ln_delta3),na.rm=T),
      nec.PC_ln_delta23avg = mean(c(nec.PC_ln_delta2,nec.PC_ln_delta3),na.rm=T),
      fire.PC_ln_delta23avg = mean(c(fire.PC_ln_delta2,fire.PC_ln_delta3),na.rm=T),
      police.PC_ln_delta23avg = mean(c(police.PC_ln_delta2,police.PC_ln_delta3),na.rm=T),
      health.PC_ln_delta23avg = mean(c(health.PC_ln_delta2,health.PC_ln_delta3),na.rm=T),
      highways.PC_ln_delta23avg = mean(c(highways.PC_ln_delta2,highways.PC_ln_delta3),na.rm=T),
      parking.PC_ln_delta23avg = mean(c(parking.PC_ln_delta2,parking.PC_ln_delta3),na.rm=T),
      housing.PC_ln_delta23avg = mean(c(housing.PC_ln_delta2,housing.PC_ln_delta3),na.rm=T),
      libraries.PC_ln_delta23avg = mean(c(libraries.PC_ln_delta2,libraries.PC_ln_delta3),na.rm=T),
      parks.PC_ln_delta23avg = mean(c(parks.PC_ln_delta2,parks.PC_ln_delta3),na.rm=T),
      sanitation.PC_ln_delta23avg = mean(c(sanitation.PC_ln_delta2,sanitation.PC_ln_delta3),na.rm=T),
      utilities.PC_ln_delta23avg = mean(c(utilities.PC_ln_delta2,utilities.PC_ln_delta3),na.rm=T),
      welfare.PC_ln_delta23avg = mean(c(welfare.PC_ln_delta2,welfare.PC_ln_delta3),na.rm=T),
      interest.PC_ln_delta23avg = mean(c(interest.PC_ln_delta2,interest.PC_ln_delta3),na.rm=T),
      admin.PC_ln_delta23avg = mean(c(admin.PC_ln_delta2,admin.PC_ln_delta3),na.rm=T),
      revenue.PC_ln_delta23avg = mean(c(revenue.PC_ln_delta2,revenue.PC_ln_delta3),na.rm=T),
      revenueown.PC_ln_delta23avg = mean(c(revenueown.PC_ln_delta2,revenueown.PC_ln_delta3),na.rm=T),
      revenuegeneral.PC_ln_delta23avg = mean(c(revenuegeneral.PC_ln_delta2,revenuegeneral.PC_ln_delta3),na.rm=T),
      revenueowngeneral.PC_ln_delta23avg = mean(c(revenueowngeneral.PC_ln_delta2,revenueowngeneral.PC_ln_delta3),na.rm=T),
      taxes.PC_ln_delta23avg = mean(c(taxes.PC_ln_delta2,taxes.PC_ln_delta3),na.rm=T),
      propertytax.PC_ln_delta23avg = mean(c(propertytax.PC_ln_delta2,propertytax.PC_ln_delta3),na.rm=T),
      salestax.select.PC_ln_delta23avg = mean(c(salestax.select.PC_ln_delta2,salestax.select.PC_ln_delta3),na.rm=T),
      salestax.general.PC_ln_delta23avg = mean(c(salestax.general.PC_ln_delta2,salestax.general.PC_ln_delta3),na.rm=T),
      igrev.state.PC_ln_delta23avg = mean(c(igrev.state.PC_ln_delta2,igrev.state.PC_ln_delta3),na.rm=T),
      igrev.PC_ln_delta23avg = mean(c(igrev.PC_ln_delta2,igrev.PC_ln_delta3),na.rm=T),
      charges.PC_ln_delta23avg = mean(c(charges.PC_ln_delta2,charges.PC_ln_delta3),na.rm=T),
      utilitiesrev.PC_ln_delta23avg = mean(c(utilitiesrev.PC_ln_delta2,utilitiesrev.PC_ln_delta3),na.rm=T),
      debt.PC_ln_delta23avg = mean(c(debt.PC_ln_delta2,debt.PC_ln_delta3),na.rm=T),
      generaldebt.PC_ln_delta23avg = mean(c(generaldebt.PC_ln_delta2,generaldebt.PC_ln_delta3),na.rm=T),
      LT.debt.ffc.PC_ln_delta23avg = mean(c(LT.debt.ffc.PC_ln_delta2,LT.debt.ffc.PC_ln_delta3),na.rm=T),
      LT.debt.ng.PC_ln_delta23avg = mean(c(LT.debt.ng.PC_ln_delta2,LT.debt.ng.PC_ln_delta3),na.rm=T),
      LT.debt.PC_ln_delta23avg = mean(c(LT.debt.PC_ln_delta2,LT.debt.PC_ln_delta3),na.rm=T)
    )
  
  
  IndFin_formerge <- IndFin_formerge %>%
    ungroup()
  
  IndFin_formerge_2010 <- IndFin_formerge %>%
    filter(Year4 == 2010 & !is.na(place_fips)) %>%
    select(place_fips,population_est) %>%
    rename(population_2010 = population_est)

  nrow(IndFin_formerge) # 2180813
  IndFin_formerge <- left_join(IndFin_formerge,IndFin_formerge_2010,by="place_fips")
  nrow(IndFin_formerge) # 2180813

  IndFin_formerge_2020 <- IndFin_formerge %>%
    filter(Year4 == 2020 & !is.na(place_fips)) %>%
    select(place_fips,population_est) %>%
    rename(population_2020 = population_est)
  
  nrow(IndFin_formerge) # 2180813
  IndFin_formerge <- left_join(IndFin_formerge,IndFin_formerge_2020,by="place_fips")
  nrow(IndFin_formerge) # 2180813
  
  
  IndFin_formerge <- IndFin_formerge %>%
    rowwise() %>%
    mutate(Total.Allocational.PC = sum(c(fire.PC,police.PC,corrections.PC,interest.PC,admin.PC,nec.PC,parks.PC,naturalresources.PC,libraries.PC,sanitation.PC),na.rm=T), 
           Total.Developmental.PC = sum(c(highways.PC,utilities.PC,parking.PC),na.rm=T),
           Total.Redistribution.PC = sum(c(health.PC,hospitals.PC,welfare.PC,housing.PC),na.rm=T), 
           Total.PublicProtection.PC = sum(c(fire.PC,police.PC,corrections.PC),na.rm=T), 
           Total.AdminMisc.PC = sum(c(admin.PC,nec.PC),na.rm=T), 
           Total.RoadsParking.PC = sum(c(highways.PC,parking.PC),na.rm=T), 
           Total.SanitationUtilities.PC = sum(c(sanitation.PC,utilities.PC),na.rm=T), 
           Total.RoadsSanitationUtilities.PC = sum(c(sanitation.PC,utilities.PC,highways.PC),na.rm=T), 
           Total.EducationLibraries.PC = sum(c(education.PC,libraries.PC),na.rm=T), 
           Total.ParksResources.PC = sum(c(parks.PC,naturalresources.PC),na.rm=T)
    )
  
  
  IndFin_formerge <- IndFin_formerge %>%
    group_by(place_fips) %>%
    mutate(
      Total.Allocational.PC_ln = log1p(Total.Allocational.PC),
      Total.Allocational.PC_ln_lead1 = lead.new(Total.Allocational.PC_ln,n=1,along_with=Year4),
      Total.Allocational.PC_ln_lead2 = lead.new(Total.Allocational.PC_ln,n=2,along_with=Year4),
      Total.Allocational.PC_ln_lead3 = lead.new(Total.Allocational.PC_ln,n=3,along_with=Year4),
      Total.Allocational.PC_ln_lead4 = lead.new(Total.Allocational.PC_ln,n=4,along_with=Year4),
      Total.Allocational.PC_ln_lead5 = lead.new(Total.Allocational.PC_ln,n=5,along_with=Year4),
      Total.Allocational.PC_ln_delta1 = Total.Allocational.PC_ln_lead1 - Total.Allocational.PC_ln,
      Total.Allocational.PC_ln_delta2 = Total.Allocational.PC_ln_lead2 - Total.Allocational.PC_ln,
      Total.Allocational.PC_ln_delta3 = Total.Allocational.PC_ln_lead3 - Total.Allocational.PC_ln,
      
      Total.Developmental.PC_ln = log1p(Total.Developmental.PC),
      Total.Developmental.PC_ln_lead1 = lead.new(Total.Developmental.PC_ln,n=1,along_with=Year4),
      Total.Developmental.PC_ln_lead2 = lead.new(Total.Developmental.PC_ln,n=2,along_with=Year4),
      Total.Developmental.PC_ln_lead3 = lead.new(Total.Developmental.PC_ln,n=3,along_with=Year4),
      Total.Developmental.PC_ln_lead4 = lead.new(Total.Developmental.PC_ln,n=4,along_with=Year4),
      Total.Developmental.PC_ln_lead5 = lead.new(Total.Developmental.PC_ln,n=5,along_with=Year4),
      Total.Developmental.PC_ln_delta1 = Total.Developmental.PC_ln_lead1 - Total.Developmental.PC_ln,
      Total.Developmental.PC_ln_delta2 = Total.Developmental.PC_ln_lead2 - Total.Developmental.PC_ln,
      Total.Developmental.PC_ln_delta3 = Total.Developmental.PC_ln_lead3 - Total.Developmental.PC_ln,
      
      Total.Redistribution.PC_ln = log1p(Total.Redistribution.PC),
      Total.Redistribution.PC_ln_lead1 = lead.new(Total.Redistribution.PC_ln,n=1,along_with=Year4),
      Total.Redistribution.PC_ln_lead2 = lead.new(Total.Redistribution.PC_ln,n=2,along_with=Year4),
      Total.Redistribution.PC_ln_lead3 = lead.new(Total.Redistribution.PC_ln,n=3,along_with=Year4),
      Total.Redistribution.PC_ln_lead4 = lead.new(Total.Redistribution.PC_ln,n=4,along_with=Year4),
      Total.Redistribution.PC_ln_lead5 = lead.new(Total.Redistribution.PC_ln,n=5,along_with=Year4),
      Total.Redistribution.PC_ln_delta1 = Total.Redistribution.PC_ln_lead1 - Total.Redistribution.PC_ln,
      Total.Redistribution.PC_ln_delta2 = Total.Redistribution.PC_ln_lead2 - Total.Redistribution.PC_ln,
      Total.Redistribution.PC_ln_delta3 = Total.Redistribution.PC_ln_lead3 - Total.Redistribution.PC_ln,
      
      Total.PublicProtection.PC_ln = log1p(Total.PublicProtection.PC),
      Total.PublicProtection.PC_ln_lead1 = lead.new(Total.PublicProtection.PC_ln,n=1,along_with=Year4),
      Total.PublicProtection.PC_ln_lead2 = lead.new(Total.PublicProtection.PC_ln,n=2,along_with=Year4),
      Total.PublicProtection.PC_ln_lead3 = lead.new(Total.PublicProtection.PC_ln,n=3,along_with=Year4),
      Total.PublicProtection.PC_ln_lead4 = lead.new(Total.PublicProtection.PC_ln,n=4,along_with=Year4),
      Total.PublicProtection.PC_ln_lead5 = lead.new(Total.PublicProtection.PC_ln,n=5,along_with=Year4),
      Total.PublicProtection.PC_ln_delta1 = Total.PublicProtection.PC_ln_lead1 - Total.PublicProtection.PC_ln,
      Total.PublicProtection.PC_ln_delta2 = Total.PublicProtection.PC_ln_lead2 - Total.PublicProtection.PC_ln,
      Total.PublicProtection.PC_ln_delta3 = Total.PublicProtection.PC_ln_lead3 - Total.PublicProtection.PC_ln,
      
      Total.AdminMisc.PC_ln = log1p(Total.AdminMisc.PC),
      Total.AdminMisc.PC_ln_lead1 = lead.new(Total.AdminMisc.PC_ln,n=1,along_with=Year4),
      Total.AdminMisc.PC_ln_lead2 = lead.new(Total.AdminMisc.PC_ln,n=2,along_with=Year4),
      Total.AdminMisc.PC_ln_lead3 = lead.new(Total.AdminMisc.PC_ln,n=3,along_with=Year4),
      Total.AdminMisc.PC_ln_lead4 = lead.new(Total.AdminMisc.PC_ln,n=4,along_with=Year4),
      Total.AdminMisc.PC_ln_lead5 = lead.new(Total.AdminMisc.PC_ln,n=5,along_with=Year4),
      Total.AdminMisc.PC_ln_delta1 = Total.AdminMisc.PC_ln_lead1 - Total.AdminMisc.PC_ln,
      Total.AdminMisc.PC_ln_delta2 = Total.AdminMisc.PC_ln_lead2 - Total.AdminMisc.PC_ln,
      Total.AdminMisc.PC_ln_delta3 = Total.AdminMisc.PC_ln_lead3 - Total.AdminMisc.PC_ln,
      
      Total.RoadsParking.PC_ln = log1p(Total.RoadsParking.PC),
      Total.RoadsParking.PC_ln_lead1 = lead.new(Total.RoadsParking.PC_ln,n=1,along_with=Year4),
      Total.RoadsParking.PC_ln_lead2 = lead.new(Total.RoadsParking.PC_ln,n=2,along_with=Year4),
      Total.RoadsParking.PC_ln_lead3 = lead.new(Total.RoadsParking.PC_ln,n=3,along_with=Year4),
      Total.RoadsParking.PC_ln_lead4 = lead.new(Total.RoadsParking.PC_ln,n=4,along_with=Year4),
      Total.RoadsParking.PC_ln_lead5 = lead.new(Total.RoadsParking.PC_ln,n=5,along_with=Year4),
      Total.RoadsParking.PC_ln_delta1 = Total.RoadsParking.PC_ln_lead1 - Total.RoadsParking.PC_ln,
      Total.RoadsParking.PC_ln_delta2 = Total.RoadsParking.PC_ln_lead2 - Total.RoadsParking.PC_ln,
      Total.RoadsParking.PC_ln_delta3 = Total.RoadsParking.PC_ln_lead3 - Total.RoadsParking.PC_ln,
      
      Total.SanitationUtilities.PC_ln = log1p(Total.SanitationUtilities.PC),
      Total.SanitationUtilities.PC_ln_lead1 = lead.new(Total.SanitationUtilities.PC_ln,n=1,along_with=Year4),
      Total.SanitationUtilities.PC_ln_lead2 = lead.new(Total.SanitationUtilities.PC_ln,n=2,along_with=Year4),
      Total.SanitationUtilities.PC_ln_lead3 = lead.new(Total.SanitationUtilities.PC_ln,n=3,along_with=Year4),
      Total.SanitationUtilities.PC_ln_lead4 = lead.new(Total.SanitationUtilities.PC_ln,n=4,along_with=Year4),
      Total.SanitationUtilities.PC_ln_lead5 = lead.new(Total.SanitationUtilities.PC_ln,n=5,along_with=Year4),
      Total.SanitationUtilities.PC_ln_delta1 = Total.SanitationUtilities.PC_ln_lead1 - Total.SanitationUtilities.PC_ln,
      Total.SanitationUtilities.PC_ln_delta2 = Total.SanitationUtilities.PC_ln_lead2 - Total.SanitationUtilities.PC_ln,
      Total.SanitationUtilities.PC_ln_delta3 = Total.SanitationUtilities.PC_ln_lead3 - Total.SanitationUtilities.PC_ln,
      
      Total.RoadsSanitationUtilities.PC_ln = log1p(Total.RoadsSanitationUtilities.PC),
      Total.RoadsSanitationUtilities.PC_ln_lead1 = lead.new(Total.RoadsSanitationUtilities.PC_ln,n=1,along_with=Year4),
      Total.RoadsSanitationUtilities.PC_ln_lead2 = lead.new(Total.RoadsSanitationUtilities.PC_ln,n=2,along_with=Year4),
      Total.RoadsSanitationUtilities.PC_ln_lead3 = lead.new(Total.RoadsSanitationUtilities.PC_ln,n=3,along_with=Year4),
      Total.RoadsSanitationUtilities.PC_ln_lead4 = lead.new(Total.RoadsSanitationUtilities.PC_ln,n=4,along_with=Year4),
      Total.RoadsSanitationUtilities.PC_ln_lead5 = lead.new(Total.RoadsSanitationUtilities.PC_ln,n=5,along_with=Year4),
      Total.RoadsSanitationUtilities.PC_ln_delta1 = Total.RoadsSanitationUtilities.PC_ln_lead1 - Total.RoadsSanitationUtilities.PC_ln,
      Total.RoadsSanitationUtilities.PC_ln_delta2 = Total.RoadsSanitationUtilities.PC_ln_lead2 - Total.RoadsSanitationUtilities.PC_ln,
      Total.RoadsSanitationUtilities.PC_ln_delta3 = Total.RoadsSanitationUtilities.PC_ln_lead3 - Total.RoadsSanitationUtilities.PC_ln,
      
      Total.EducationLibraries.PC_ln = log1p(Total.EducationLibraries.PC),
      Total.EducationLibraries.PC_ln_lead1 = lead.new(Total.EducationLibraries.PC_ln,n=1,along_with=Year4),
      Total.EducationLibraries.PC_ln_lead2 = lead.new(Total.EducationLibraries.PC_ln,n=2,along_with=Year4),
      Total.EducationLibraries.PC_ln_lead3 = lead.new(Total.EducationLibraries.PC_ln,n=3,along_with=Year4),
      Total.EducationLibraries.PC_ln_lead4 = lead.new(Total.EducationLibraries.PC_ln,n=4,along_with=Year4),
      Total.EducationLibraries.PC_ln_lead5 = lead.new(Total.EducationLibraries.PC_ln,n=5,along_with=Year4),
      Total.EducationLibraries.PC_ln_delta1 = Total.EducationLibraries.PC_ln_lead1 - Total.EducationLibraries.PC_ln,
      Total.EducationLibraries.PC_ln_delta2 = Total.EducationLibraries.PC_ln_lead2 - Total.EducationLibraries.PC_ln,
      Total.EducationLibraries.PC_ln_delta3 = Total.EducationLibraries.PC_ln_lead3 - Total.EducationLibraries.PC_ln,
      
      Total.ParksResources.PC_ln = log1p(Total.ParksResources.PC),
      Total.ParksResources.PC_ln_lead1 = lead.new(Total.ParksResources.PC_ln,n=1,along_with=Year4),
      Total.ParksResources.PC_ln_lead2 = lead.new(Total.ParksResources.PC_ln,n=2,along_with=Year4),
      Total.ParksResources.PC_ln_lead3 = lead.new(Total.ParksResources.PC_ln,n=3,along_with=Year4),
      Total.ParksResources.PC_ln_lead4 = lead.new(Total.ParksResources.PC_ln,n=4,along_with=Year4),
      Total.ParksResources.PC_ln_lead5 = lead.new(Total.ParksResources.PC_ln,n=5,along_with=Year4),
      Total.ParksResources.PC_ln_delta1 = Total.ParksResources.PC_ln_lead1 - Total.ParksResources.PC_ln,
      Total.ParksResources.PC_ln_delta2 = Total.ParksResources.PC_ln_lead2 - Total.ParksResources.PC_ln,
      Total.ParksResources.PC_ln_delta3 = Total.ParksResources.PC_ln_lead3 - Total.ParksResources.PC_ln
      
    )
  
  
  
  IndFin_formerge <- IndFin_formerge %>%
    rowwise() %>%
    mutate(
      Total.Allocational.PC_ln_delta23avg = mean(c(Total.Allocational.PC_ln_delta2,Total.Allocational.PC_ln_delta3),na.rm=T),
      Total.Developmental.PC_ln_delta23avg = mean(c(Total.Developmental.PC_ln_delta2,Total.Developmental.PC_ln_delta3),na.rm=T),
      Total.Redistribution.PC_ln_delta23avg = mean(c(Total.Redistribution.PC_ln_delta2,Total.Redistribution.PC_ln_delta3),na.rm=T),
      Total.PublicProtection.PC_ln_delta23avg = mean(c(Total.PublicProtection.PC_ln_delta2,Total.PublicProtection.PC_ln_delta3),na.rm=T),
      Total.AdminMisc.PC_ln_delta23avg = mean(c(Total.AdminMisc.PC_ln_delta2,Total.AdminMisc.PC_ln_delta3),na.rm=T),
      Total.RoadsParking.PC_ln_delta23avg = mean(c(Total.RoadsParking.PC_ln_delta2,Total.RoadsParking.PC_ln_delta3),na.rm=T),
      Total.SanitationUtilities.PC_ln_delta23avg = mean(c(Total.SanitationUtilities.PC_ln_delta2,Total.SanitationUtilities.PC_ln_delta3),na.rm=T),
      Total.RoadsSanitationUtilities.PC_ln_delta23avg = mean(c(Total.RoadsSanitationUtilities.PC_ln_delta2,Total.RoadsSanitationUtilities.PC_ln_delta3),na.rm=T),
      Total.EducationLibraries.PC_ln_delta23avg = mean(c(Total.EducationLibraries.PC_ln_delta2,Total.EducationLibraries.PC_ln_delta3),na.rm=T),
      Total.ParksResources.PC_ln_delta23avg = mean(c(Total.ParksResources.PC_ln_delta2,Total.ParksResources.PC_ln_delta3),na.rm=T)
    )
  
  write_rds(IndFin_formerge,file = "IndFin_19672020_merged_wdeltas.rds",compress = "gz")
  save(IndFin_formerge,file = "IndFin_19672020_merged_wdeltas.RData")
} 
