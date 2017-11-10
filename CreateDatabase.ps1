#####This Script will install the Database and do the First run of scoring the data.
#####It is conifgured for default settings ie Creating the Hospital Database in the SQL Server 
#####If you want to choose a different database name , you can call this script from a ps cmd line using this cmd  ./CreateDatabase.ps1 -PromptedInstall Y or Uncomment line 21 and
#####run the script from this window.
##### Created on 10.5.2017 Bob White  

param 
(
[Parameter(Mandatory=$false)] [String] $PromptedInstall  =  "",

[Parameter(Mandatory=$false)] [String] $ServerName  =  "",

[Parameter(Mandatory=$false)] [String] $dbName  =  ""
)

########################################################################
#Check Install Type Prompted Or Not Prompted, Not Prompted is Default
########################################################################
$Prompt = $PromptedInstall

#$Prompt = 'Y'
$Prompt = 
        if ($Prompt -eq 'Y' -or $Prompt -eq 'y') {'Y'} 
        elseif ([string]::IsNullOrEmpty($Prompt) -or $Prompt -eq 'N' -or $Prompt -eq 'n' ) {'N'}  
######################################################################## 
# If Prompted Install is Invoked, Prompt For SQLServer and dbName
########################################################################

$ServerName = if ([string]::IsNullOrEmpty($ServerName) -and ($Prompt -eq 'Y' -Or $Prompt -eq 'y')) {Read-Host  -Prompt "Enter Desired SQL Server Name"} 
                elseif ((![string]::IsNullOrEmpty($ServerName)) -and ($Prompt -eq 'Y' -Or $Prompt -eq 'y')) {$ServerName}
                else {"LOCALHOST"}

$dbName = if ([string]::IsNullOrEmpty($dbName) -and ($Prompt -eq 'Y' -Or $Prompt -eq 'y')) {Read-Host  -Prompt "Enter Desired Database Name"} 
            elseif ((![string]::IsNullOrEmpty($dbName)) -and ($Prompt -eq 'Y' -Or $Prompt -eq 'y')) {$dbName}
            else {"Hospital"} 


    

######################################################################## 
#Decide whether we are using Trusted or Non Trusted Connections. ........Currently this does not work..............
########################################################################

$trustedConnection = "Y"
##$trustedConnection = if ($Prompt -eq 'y' -or $Prompt -eq 'Y') {"Y"} ELSE {Read-Host  -Prompt "Use Trusted Connection? Type in 'Y' or 'N'"}
##$UserName = if ($trustedConnection -eq 'n' -or $trustedConnection -eq 'N') {Read-Host  -Prompt "Enter UserName"}
##$Password = if ($trustedConnection -eq 'n' -or $trustedConnection -eq 'N') {Read-Host  -Prompt "Enter Password" -AsSecureString} 
 

$startTime= Get-Date
Write-Host "Start time is:" $startTime  


 ##$ServerName = "LOCALHOST"
 $basePath = "c:\Solutions\Hospital\"
 $dataPath = $basePath+ "Data"
 $scriptPath =  $basePath + "Resources\ActionScripts\"
 $SqlPath = $basePath + "SQLR\"
 

##########################################################################

# Create Database and BaseTables 

#########################################################################

Write-Host -ForeGroundColor 'cyan' (" Using $ServerName SQL Instance") 


$SqlParameters = @("dbName=$dbName")

$CreateSQLDB = "$ScriptPath\CreateDatabase.sql"

$CreateSQLObjects = "$ScriptPath\CreateSQLObjects.sql"
Write-Host -ForeGroundColor 'cyan' (" Calling Script to create the  $dbName database") 
invoke-sqlcmd -inputfile $CreateSQLDB -serverinstance $ServerName -database master -Variable $SqlParameters
Write-Host -ForeGroundColor 'cyan' (" SQLServerDB $dbName Created")
invoke-sqlcmd "USE $dbName;" 
Write-Host -ForeGroundColor 'cyan' (" Calling Script to create the objects in the $dbName database")
invoke-sqlcmd -inputfile $CreateSQLObjects -serverinstance $ServerName -database $dbName
Write-Host -ForeGroundColor 'cyan' (" SQLServerObjects Created in $dbName Database")


    
##########################################################################
# Deployment Pipeline
##########################################################################
   try
       {
    
        Write-Host -ForeGroundColor 'cyan' (" Import CSV File(s).")
        $dataList = "LengthOfStay"

		
		# upload csv files into SQL tables
        foreach ($dataFile in $dataList)
        {
            $destination = $dataPath + "\" + $dataFile + ".csv" 
            $tableName = $DBName + ".dbo." + $dataFile
            $tableSchema = $dataPath + "\" + $dataFile + ".xml"
            $dataSet = Import-Csv $destination
         Write-Host -ForegroundColor 'cyan' ("         Loading $dataFile.csv into SQL Table, this will take about 30 seconds per file....") 
            Write-SqlTableData -InputData $dataSet  -DatabaseName $dbName -Force -Passthru -SchemaName dbo -ServerInstance $ServerName -TableName $dataFile
            
         Write-Host -ForeGroundColor 'cyan' (" $datafile table loaded from CSV File(s).")
        }
    }
    catch
    {
        Write-Host -ForegroundColor DarkYellow "Exception in populating database tables:"
        Write-Host -ForegroundColor Red $Error[0].Exception 
        throw
    }
    Write-Host -ForeGroundColor 'cyan' (" Finished loading .csv File(s).")
    
    
    # compute statistics for production and faster NA replacement.
    Write-Host -ForeGroundColor 'Cyan' (" Computing statistics on the input table...")
    $query = "EXEC compute_stats"
    if($trustedConnection -eq 'Y' -or $trustedConnection -eq 'y') 
        {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -Query $query}
        ELSE {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -User $UserName -Password $Password  -Query $query}
        #ELSE {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -Credential $Credential  -Query $query}
    

    # execute the NA replacement
    $Replace = if ($Prompt -eq 'y' -or $Prompt -eq 'Y') 
        {Read-Host -Prompt " Replacing missing values with mode and mean [M/m] or with missing and -1 [miss]?' Type in 'Y' or 'N' "}
        ELSE {"N"}
    if ($Replace -eq 'Y' -or $Replace -eq 'y') {Write-Host -ForeGroundColor 'Cyan' (" Replacing missing values with the mean and mode...")}
                    ELSE {Write-Host -ForeGroundColor 'Cyan' (" Not Replacing missing values with the mean and mode...")}
            
    $query = if ($Replace -eq 'N' -or $Replace -eq 'n') {"EXEC fill_NA_mode_mean 'LengthOfStay', 'LoS0'"}  ELSE {"EXEC fill_NA_explicit 'LengthOfStay', 'LoS0'"} 
    if($trustedConnection -eq 'Y' -or $trustedConnection -eq 'y') 
        {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -Query $query}
        ELSE {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -User $UserName -Password $Password  -Query $query}
  

    # execute the feature engineering
    Write-Host -ForeGroundColor 'Cyan' (" Computing new features...")
    $query = "EXEC feature_engineering 'LoS0', 'LoS', 0"
    if($trustedConnection -eq 'Y' -or $trustedConnection -eq 'y') 
        {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -Query $query}
        ELSE {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -User $UserName -Password $Password  -Query $query}

    # get the column information
    Write-Host -ForeGroundColor 'Cyan' (" Getting column information...")
    $query = "EXEC get_column_info 'LoS'"
    if($trustedConnection -eq 'Y' -or $trustedConnection -eq 'y') 
        {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -Query $query}
        ELSE {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -User $UserName -Password $Password  -Query $query}
    

    # execute the procedure
    #$splitting_percent = 70
    $splitting_percent = if ($Prompt -eq 'n' -or $Prompt -eq 'N') {"70"} ELSE {Read-Host ' Split Percent (e.g. Type 70 for 70% in training set) ?'}
    Write-Host -ForeGroundColor 'Cyan' (" Splitting the data set at $splitting_percent%...")
    $query = "EXEC splitting $splitting_percent, 'LoS'"
    if($trustedConnection -eq 'Y' -or $trustedConnection -eq 'y') 
        {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -Query $query}
        ELSE {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -User $UserName -Password $Password  -Query $query}


    # execute the training 
    Write-Host -ForeGroundColor 'Cyan' (" Training Gradient Boosted Trees (rxFastTrees implementation)...")
    $modelName = 'GBT'
    $query = "EXEC train_model $modelName, 'LoS'"
    if($trustedConnection -eq 'Y' -or $trustedConnection -eq 'y') 
        {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -Query $query}
        ELSE {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -User $UserName -Password $Password  -Query $query}
     

    # execute the scoring 
    Write-Host -ForeGroundColor 'Cyan' (" Scoring Gradient Boosted Trees (rxFastTrees implementation)...")
    $query = "EXEC score $modelName, 'SELECT * FROM LoS WHERE eid NOT IN (SELECT eid FROM Train_Id)', 'Boosted_Prediction'"
    if($trustedConnection -eq 'Y' -or $trustedConnection -eq 'y') 
        {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -Query $query}
        ELSE {Invoke-Sqlcmd -ServerInstance $ServerName -Database  $dbName -User $UserName -Password $Password  -Query $query}


    # execute the evaluation 
    Write-Host -ForeGroundColor 'Cyan' (" Evaluating Gradient Boosted Trees (rxFastTrees implementation) ...")
    $query = "EXEC evaluate $modelName, 'Boosted_Prediction'"
    if($trustedConnection -eq 'Y' -or $trustedConnection -eq 'y') 
        {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -Query $query}
        ELSE {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -User $UserName -Password $Password  -Query $query}
   

   
    Write-Host -ForeGroundColor 'Cyan' (" Execute Prediction Results ...")
    $query = "EXEC prediction_results"
    if($trustedConnection -eq 'Y' -or $trustedConnection -eq 'y') 
        {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -Query $query}
        ELSE {Invoke-Sqlcmd -ServerInstance $ServerName -Database $dbName -User $UserName -Password $Password  -Query $query}

    $endTime= Get-Date
    Write-Host "Start time is:" $endTime
    $Duration = New-TimeSpan -Start $StartTime -End $EndTime 
    Write-Host -ForegroundColor 'green'("Total Deployment Time = $Duration") 
    Write-Host -foregroundcolor 'green'("Length of Stay Development Workflow Finished Successfully!")
 
