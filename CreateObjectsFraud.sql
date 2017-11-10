USE [Fraud]
GO
ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = OFF;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET LEGACY_CARDINALITY_ESTIMATION = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET MAXDOP = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SNIFFING = ON;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET PARAMETER_SNIFFING = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET QUERY_OPTIMIZER_HOTFIXES = OFF;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET QUERY_OPTIMIZER_HOTFIXES = PRIMARY;
GO
USE [Fraud]
GO
/****** Object:  UserDefinedFunction [dbo].[FormatTime]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[FormatTime] (@strTime varchar(255) ) 
returns varchar(255)
as
begin
  declare @strTimeNew varchar(255)
  set @strTimeNew = 
  case
    when len(@strTime) = 5 then concat('0',@strTime)
    when len(@strTime) = 4 then concat('00',@strTime)
    when len(@strTime) = 3 then concat('000',@strTime)
    when len(@strTime) = 2 then concat('0000',@strTime)
    when len(@strTime) = 1 then concat('00000',@strTime)
   else @strTime
  end
  return(@strTimeNew)
end
GO
/****** Object:  Table [dbo].[Tagged_Training]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Tagged_Training](
	[transactionID] [varchar](255) NULL,
	[accountID] [varchar](255) NULL,
	[transactionAmountUSD] [varchar](255) NULL,
	[transactionAmount] [varchar](255) NULL,
	[transactionCurrencyCode] [varchar](255) NULL,
	[transactionCurrencyConversionRate] [varchar](255) NULL,
	[transactionDate] [varchar](255) NULL,
	[transactionTime] [varchar](255) NULL,
	[localHour] [varchar](255) NULL,
	[transactionScenario] [varchar](255) NULL,
	[transactionType] [varchar](255) NULL,
	[transactionMethod] [varchar](255) NULL,
	[transactionDeviceType] [varchar](255) NULL,
	[transactionDeviceId] [varchar](255) NULL,
	[transactionIPaddress] [varchar](255) NULL,
	[ipState] [varchar](255) NULL,
	[ipPostcode] [varchar](255) NULL,
	[ipCountryCode] [varchar](255) NULL,
	[isProxyIP] [varchar](255) NULL,
	[browserType] [varchar](255) NULL,
	[browserLanguage] [varchar](255) NULL,
	[paymentInstrumentType] [varchar](255) NULL,
	[cardType] [varchar](255) NULL,
	[cardNumberInputMethod] [varchar](255) NULL,
	[paymentInstrumentID] [varchar](255) NULL,
	[paymentBillingAddress] [varchar](255) NULL,
	[paymentBillingPostalCode] [varchar](255) NULL,
	[paymentBillingState] [varchar](255) NULL,
	[paymentBillingCountryCode] [varchar](255) NULL,
	[paymentBillingName] [varchar](255) NULL,
	[shippingAddress] [varchar](255) NULL,
	[shippingPostalCode] [varchar](255) NULL,
	[shippingCity] [varchar](255) NULL,
	[shippingState] [varchar](255) NULL,
	[shippingCountry] [varchar](255) NULL,
	[cvvVerifyResult] [varchar](255) NULL,
	[responseCode] [varchar](255) NULL,
	[digitalItemCount] [varchar](255) NULL,
	[physicalItemCount] [varchar](255) NULL,
	[purchaseProductType] [varchar](255) NULL,
	[transactionDateTime] [datetime] NULL,
	[accountOwnerName] [varchar](255) NULL,
	[accountAddress] [varchar](255) NULL,
	[accountPostalCode] [varchar](255) NULL,
	[accountCity] [varchar](255) NULL,
	[accountState] [varchar](255) NULL,
	[accountCountry] [varchar](255) NULL,
	[accountOpenDate] [varchar](255) NULL,
	[accountAge] [varchar](255) NULL,
	[isUserRegistered] [varchar](255) NULL,
	[paymentInstrumentAgeInAccount] [varchar](255) NULL,
	[numPaymentRejects1dPerUser] [varchar](255) NULL,
	[tDT] [datetime] NULL,
	[sDT] [datetime] NULL,
	[eDT] [datetime] NULL,
	[label] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[Tagged_Training_Processed]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[Tagged_Training_Processed] as
select
label,
accountID,
transactionID,
transactionDateTime,
isnull(isProxyIP, '0') as isProxyIP, 
isnull(paymentInstrumentType, '0') as paymentInstrumentType,
isnull(cardType, '0') as cardType,
isnull(paymentBillingAddress, '0') as paymentBillingAddress,
isnull(paymentBillingPostalCode, '0') as paymentBillingPostalCode,
isnull(paymentBillingCountryCode, '0') as paymentBillingCountryCode,
isnull(paymentBillingName, '0') as paymentBillingName,
isnull(accountAddress, '0') as accountAddress,
isnull(accountPostalCode, '0') as accountPostalCode,
isnull(accountCountry, '0') as accountCountry,
isnull(accountOwnerName, '0') as accountOwnerName,
isnull(shippingAddress, '0') as shippingAddress,
isnull(transactionCurrencyCode, '0') as transactionCurrencyCode,
isnull(localHour,'-99') as localHour,
isnull(ipState, '0') as ipState,
isnull(ipPostCode, '0') as ipPostCode,
isnull(ipCountryCode, '0') as ipCountryCode,
isnull(browserLanguage, '0') as browserLanguage,
isnull(paymentBillingState, '0') as paymentBillingState,
isnull(accountState, '0') as accountState,
case when isnumeric(transactionAmountUSD)=1 then cast(transactionAmountUSD as float) else 0 end as transactionAmountUSD,
case when isnumeric(digitalItemCount)=1 then cast(digitalItemCount as float) else 0 end as digitalItemCount,
case when isnumeric(physicalItemCount)=1 then cast(physicalItemCount as float) else 0 end as physicalItemCount,
case when isnumeric(accountAge)=1 then cast(accountAge as float) else 0 end as accountAge,
case when isnumeric(paymentInstrumentAgeInAccount)=1 then cast(paymentInstrumentAgeInAccount as float) else 0 end as paymentInstrumentAgeInAccount,
case when isnumeric(numPaymentRejects1dPerUser)=1 then cast(numPaymentRejects1dPerUser as float) else 0 end as numPaymentRejects1dPerUser,
isUserRegistered = case when isUserRegistered like '%[0-9]%' then '0' else isUserRegistered end
from Tagged_Training
where cast(transactionAmountUSD as float) >= 0 and   
      (case when transactionDateTime is null then 1 else 0 end) = 0 and
	  label < 2
GO
/****** Object:  Table [dbo].[Risk_TransactionCurrencyCode]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Risk_TransactionCurrencyCode](
	[transactionCurrencyCode] [varchar](255) NOT NULL,
	[risk] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Risk_LocalHour]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Risk_LocalHour](
	[localHour] [varchar](255) NOT NULL,
	[risk] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Risk_IpState]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Risk_IpState](
	[ipState] [varchar](255) NOT NULL,
	[risk] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Risk_IpPostCode]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Risk_IpPostCode](
	[ipPostCode] [varchar](255) NOT NULL,
	[risk] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Risk_IpCountryCode]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Risk_IpCountryCode](
	[ipCountryCode] [varchar](255) NOT NULL,
	[risk] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Risk_BrowserLanguage]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Risk_BrowserLanguage](
	[browserLanguage] [varchar](255) NOT NULL,
	[risk] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Risk_PaymentBillingPostalCode]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Risk_PaymentBillingPostalCode](
	[paymentBillingPostalCode] [varchar](255) NOT NULL,
	[risk] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Risk_PaymentBillingState]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Risk_PaymentBillingState](
	[paymentBillingState] [varchar](255) NOT NULL,
	[risk] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Risk_PaymentBillingCountryCode]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Risk_PaymentBillingCountryCode](
	[paymentBillingCountryCode] [varchar](255) NOT NULL,
	[risk] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Risk_AccountPostalCode]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Risk_AccountPostalCode](
	[accountPostalCode] [varchar](255) NOT NULL,
	[risk] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Risk_AccountState]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Risk_AccountState](
	[accountState] [varchar](255) NOT NULL,
	[risk] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Risk_AccountCountry]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Risk_AccountCountry](
	[accountCountry] [varchar](255) NOT NULL,
	[risk] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[Tagged_Training_Processed_Features1]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[Tagged_Training_Processed_Features1] as
select t.label,t.accountID,t.transactionID,t.transactionDateTime,
t.transactionAmountUSD,
t.digitalItemCount,
t.physicalItemCount,
t.isProxyIP,
t.paymentInstrumentType,
t.cardType,
t.isUserRegistered,
t.accountAge,
t.paymentInstrumentAgeInAccount,
t.numPaymentRejects1dPerUser,
case when t.transactionAmountUSD > 150 then '1' else '0' end as isHighAmount,
case when t.paymentBillingAddress = t.accountAddress then '0' else '1' end as acctBillingAddressMismatchFlag,
case when t.paymentBillingPostalCode = t.accountPostalCode then '0' else '1' end as acctBillingPostalCodeMismatchFlag,
case when t.paymentBillingCountryCode = t.accountCountry then '0' else '1' end as acctBillingCountryMismatchFlag,
case when t.paymentBillingName = t.accountOwnerName then '0' else '1' end as acctBillingNameMismatchFlag,
case when t.shippingAddress = t.accountAddress then '0' else '1' end as acctShippingAddressMismatchFlag,
case when t.shippingAddress = t.paymentBillingAddress then '0' else '1' end as shippingBillingAddressMismatchFlag,
isnull(ac.risk,0) as accountCountryRisk,
isnull(apc.risk,0) as accountPostalCodeRisk,
isnull(actst.risk,0) as accountStateRisk,
isnull(bl.risk,0) as browserLanguageRisk,
isnull(ic.risk,0) as ipCountryCodeRisk,
isnull(ipc.risk,0) as ipPostCodeRisk,
isnull(ips.risk,0) as ipStateRisk,
isnull(lh.risk,0) as localHourRisk,
isnull(pbcc.risk,0) as paymentBillingCountryCodeRisk,
isnull(pbpc.risk,0) as paymentBillingPostalCodeRisk,
isnull(pbst.risk,0) as paymentBillingStateRisk,
isnull(tcc.risk,0) as transactionCurrencyCodeRisk
from Tagged_Training_Processed as t
left join Risk_AccountCountry as ac on ac.accountCountry = t.accountCountry
left join Risk_AccountPostalCode as apc on apc.accountPostalCode = t.accountPostalCode
left join Risk_AccountState as actst on actst.accountState = t.accountState
left join Risk_BrowserLanguage as bl on bl.browserLanguage = t.browserLanguage
left join Risk_IpCountryCode as ic on ic.ipCountryCode = t.ipCountryCode
left join Risk_IpPostCode as ipc on ipc.ipPostCode = t.ipPostCode
left join Risk_IpState as ips on ips.ipState = t.ipState
left join Risk_LocalHour as lh on lh.localHour = t.localHour
left join Risk_PaymentBillingCountryCode as pbcc on pbcc.paymentBillingCountryCode = t.paymentBillingCountryCode
left join Risk_PaymentBillingPostalCode as pbpc on pbpc.paymentBillingPostalCode = t.paymentBillingPostalCode
left join Risk_PaymentBillingState as pbst on pbst.paymentBillingState = t.paymentBillingState
left join Risk_TransactionCurrencyCode as tcc on tcc.transactionCurrencyCode = t.transactionCurrencyCode
GO
/****** Object:  Table [dbo].[Transaction_History]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Transaction_History](
	[accountID] [varchar](255) NULL,
	[transactionID] [varchar](255) NULL,
	[transactionDateTime] [datetime] NULL,
	[transactionAmountUSD] [varchar](255) NULL
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[Tagged_Training_Processed_Features]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[Tagged_Training_Processed_Features] as
select * from Tagged_Training_Processed_Features1 as t
outer apply
(select 
isnull(sum(case when t2.transactionDateTime > last24Hours then cast(t2.transactionAmountUSD as float) end),0) as sumPurchaseAmount1dPerUser,
isnull(count(case when t2.transactionDateTime > last24Hours then t2.transactionAmountUSD end),0) as sumPurchaseCount1dPerUser,
isnull(sum(cast(t2.transactionAmountUSD as float)),0) as sumPurchaseAmount30dPerUser,
isnull(count(t2.transactionAmountUSD),0) as sumPurchaseCount30dPerUser
from Transaction_History as t2
cross apply (values(t.transactionDateTime, DATEADD(hour, -24, t.transactionDateTime), DATEADD(day, -30, t.transactionDateTime))) as c(transactionDateTime, last24Hours, last30Days)
where t2.accountID = t.accountID and t2.transactionDateTime < t.transactionDateTime and t2.transactionDateTime > last30Days
) as a1
GO
/****** Object:  Table [dbo].[Tagged_Testing_Acct]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Tagged_Testing_Acct](
	[transactionID] [varchar](255) NULL,
	[accountID] [varchar](255) NULL,
	[transactionAmountUSD] [varchar](255) NULL,
	[transactionAmount] [varchar](255) NULL,
	[transactionCurrencyCode] [varchar](255) NULL,
	[transactionCurrencyConversionRate] [varchar](255) NULL,
	[transactionDate] [varchar](255) NULL,
	[transactionTime] [varchar](255) NULL,
	[localHour] [varchar](255) NULL,
	[transactionScenario] [varchar](255) NULL,
	[transactionType] [varchar](255) NULL,
	[transactionMethod] [varchar](255) NULL,
	[transactionDeviceType] [varchar](255) NULL,
	[transactionDeviceId] [varchar](255) NULL,
	[transactionIPaddress] [varchar](255) NULL,
	[ipState] [varchar](255) NULL,
	[ipPostcode] [varchar](255) NULL,
	[ipCountryCode] [varchar](255) NULL,
	[isProxyIP] [varchar](255) NULL,
	[browserType] [varchar](255) NULL,
	[browserLanguage] [varchar](255) NULL,
	[paymentInstrumentType] [varchar](255) NULL,
	[cardType] [varchar](255) NULL,
	[cardNumberInputMethod] [varchar](255) NULL,
	[paymentInstrumentID] [varchar](255) NULL,
	[paymentBillingAddress] [varchar](255) NULL,
	[paymentBillingPostalCode] [varchar](255) NULL,
	[paymentBillingState] [varchar](255) NULL,
	[paymentBillingCountryCode] [varchar](255) NULL,
	[paymentBillingName] [varchar](255) NULL,
	[shippingAddress] [varchar](255) NULL,
	[shippingPostalCode] [varchar](255) NULL,
	[shippingCity] [varchar](255) NULL,
	[shippingState] [varchar](255) NULL,
	[shippingCountry] [varchar](255) NULL,
	[cvvVerifyResult] [varchar](255) NULL,
	[responseCode] [varchar](255) NULL,
	[digitalItemCount] [varchar](255) NULL,
	[physicalItemCount] [varchar](255) NULL,
	[purchaseProductType] [varchar](255) NULL,
	[transactionDateTime] [datetime] NULL,
	[accountOwnerName] [varchar](255) NULL,
	[accountAddress] [varchar](255) NULL,
	[accountPostalCode] [varchar](255) NULL,
	[accountCity] [varchar](255) NULL,
	[accountState] [varchar](255) NULL,
	[accountCountry] [varchar](255) NULL,
	[accountOpenDate] [varchar](255) NULL,
	[accountAge] [varchar](255) NULL,
	[isUserRegistered] [varchar](255) NULL,
	[paymentInstrumentAgeInAccount] [varchar](255) NULL,
	[numPaymentRejects1dPerUser] [varchar](255) NULL,
	[tDT] [datetime] NULL,
	[sDT] [datetime] NULL,
	[eDT] [datetime] NULL,
	[label] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[Tagged_Testing_Acct_Processed]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[Tagged_Testing_Acct_Processed] as
select
label,
accountID,
transactionID,
transactionDateTime,
isnull(isProxyIP, '0') as isProxyIP, 
isnull(paymentInstrumentType, '0') as paymentInstrumentType,
isnull(cardType, '0') as cardType,
isnull(paymentBillingAddress, '0') as paymentBillingAddress,
isnull(paymentBillingPostalCode, '0') as paymentBillingPostalCode,
isnull(paymentBillingCountryCode, '0') as paymentBillingCountryCode,
isnull(paymentBillingName, '0') as paymentBillingName,
isnull(accountAddress, '0') as accountAddress,
isnull(accountPostalCode, '0') as accountPostalCode,
isnull(accountCountry, '0') as accountCountry,
isnull(accountOwnerName, '0') as accountOwnerName,
isnull(shippingAddress, '0') as shippingAddress,
isnull(transactionCurrencyCode, '0') as transactionCurrencyCode,
isnull(localHour,'-99') as localHour,
isnull(ipState, '0') as ipState,
isnull(ipPostCode, '0') as ipPostCode,
isnull(ipCountryCode, '0') as ipCountryCode,
isnull(browserLanguage, '0') as browserLanguage,
isnull(paymentBillingState, '0') as paymentBillingState,
isnull(accountState, '0') as accountState,
case when isnumeric(transactionAmountUSD)=1 then cast(transactionAmountUSD as float) else 0 end as transactionAmountUSD,
case when isnumeric(digitalItemCount)=1 then cast(digitalItemCount as float) else 0 end as digitalItemCount,
case when isnumeric(physicalItemCount)=1 then cast(physicalItemCount as float) else 0 end as physicalItemCount,
case when isnumeric(accountAge)=1 then cast(accountAge as float) else 0 end as accountAge,
case when isnumeric(paymentInstrumentAgeInAccount)=1 then cast(paymentInstrumentAgeInAccount as float) else 0 end as paymentInstrumentAgeInAccount,
case when isnumeric(numPaymentRejects1dPerUser)=1 then cast(numPaymentRejects1dPerUser as float) else 0 end as numPaymentRejects1dPerUser,
isUserRegistered = case when isUserRegistered like '%[0-9]%' then '0' else isUserRegistered end
from Tagged_Testing_Acct
where cast(transactionAmountUSD as float) >= 0 and   
      (case when transactionDateTime is null then 1 else 0 end) = 0 and
	  label < 2
GO
/****** Object:  View [dbo].[Tagged_Testing_Acct_Processed_Features1]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[Tagged_Testing_Acct_Processed_Features1] as
select t.label,t.accountID,t.transactionID,t.transactionDateTime,
t.transactionAmountUSD,
t.digitalItemCount,
t.physicalItemCount,
t.isProxyIP,
t.paymentInstrumentType,
t.cardType,
t.isUserRegistered,
t.accountAge,
t.paymentInstrumentAgeInAccount,
t.numPaymentRejects1dPerUser,
case when t.transactionAmountUSD > 150 then '1' else '0' end as isHighAmount,
case when t.paymentBillingAddress = t.accountAddress then '0' else '1' end as acctBillingAddressMismatchFlag,
case when t.paymentBillingPostalCode = t.accountPostalCode then '0' else '1' end as acctBillingPostalCodeMismatchFlag,
case when t.paymentBillingCountryCode = t.accountCountry then '0' else '1' end as acctBillingCountryMismatchFlag,
case when t.paymentBillingName = t.accountOwnerName then '0' else '1' end as acctBillingNameMismatchFlag,
case when t.shippingAddress = t.accountAddress then '0' else '1' end as acctShippingAddressMismatchFlag,
case when t.shippingAddress = t.paymentBillingAddress then '0' else '1' end as shippingBillingAddressMismatchFlag,
isnull(ac.risk,0) as accountCountryRisk,
isnull(apc.risk,0) as accountPostalCodeRisk,
isnull(actst.risk,0) as accountStateRisk,
isnull(bl.risk,0) as browserLanguageRisk,
isnull(ic.risk,0) as ipCountryCodeRisk,
isnull(ipc.risk,0) as ipPostCodeRisk,
isnull(ips.risk,0) as ipStateRisk,
isnull(lh.risk,0) as localHourRisk,
isnull(pbcc.risk,0) as paymentBillingCountryCodeRisk,
isnull(pbpc.risk,0) as paymentBillingPostalCodeRisk,
isnull(pbst.risk,0) as paymentBillingStateRisk,
isnull(tcc.risk,0) as transactionCurrencyCodeRisk
from Tagged_Testing_Acct_Processed as t
left join Risk_AccountCountry as ac on ac.accountCountry = t.accountCountry
left join Risk_AccountPostalCode as apc on apc.accountPostalCode = t.accountPostalCode
left join Risk_AccountState as actst on actst.accountState = t.accountState
left join Risk_BrowserLanguage as bl on bl.browserLanguage = t.browserLanguage
left join Risk_IpCountryCode as ic on ic.ipCountryCode = t.ipCountryCode
left join Risk_IpPostCode as ipc on ipc.ipPostCode = t.ipPostCode
left join Risk_IpState as ips on ips.ipState = t.ipState
left join Risk_LocalHour as lh on lh.localHour = t.localHour
left join Risk_PaymentBillingCountryCode as pbcc on pbcc.paymentBillingCountryCode = t.paymentBillingCountryCode
left join Risk_PaymentBillingPostalCode as pbpc on pbpc.paymentBillingPostalCode = t.paymentBillingPostalCode
left join Risk_PaymentBillingState as pbst on pbst.paymentBillingState = t.paymentBillingState
left join Risk_TransactionCurrencyCode as tcc on tcc.transactionCurrencyCode = t.transactionCurrencyCode
GO
/****** Object:  View [dbo].[Tagged_Testing_Acct_Processed_Features]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[Tagged_Testing_Acct_Processed_Features] as
select * from Tagged_Testing_Acct_Processed_Features1 as t
outer apply
(select 
isnull(sum(case when t2.transactionDateTime > last24Hours then cast(t2.transactionAmountUSD as float) end),0) as sumPurchaseAmount1dPerUser,
isnull(count(case when t2.transactionDateTime > last24Hours then t2.transactionAmountUSD end),0) as sumPurchaseCount1dPerUser,
isnull(sum(cast(t2.transactionAmountUSD as float)),0) as sumPurchaseAmount30dPerUser,
isnull(count(t2.transactionAmountUSD),0) as sumPurchaseCount30dPerUser
from Transaction_History as t2
cross apply (values(t.transactionDateTime, DATEADD(hour, -24, t.transactionDateTime), DATEADD(day, -30, t.transactionDateTime))) as c(transactionDateTime, last24Hours, last30Days)
where t2.accountID = t.accountID and t2.transactionDateTime < t.transactionDateTime and t2.transactionDateTime > last30Days
) as a1
GO
/****** Object:  Table [dbo].[Parsed_String_Acct]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Parsed_String_Acct](
	[transactionID] [varchar](100) NULL,
	[accountID] [varchar](100) NULL,
	[transactionAmountUSD] [varchar](100) NULL,
	[transactionAmount] [varchar](100) NULL,
	[transactionCurrencyCode] [varchar](100) NULL,
	[transactionCurrencyConversionRate] [varchar](100) NULL,
	[transactionDate] [varchar](100) NULL,
	[transactionTime] [varchar](100) NULL,
	[localHour] [varchar](100) NULL,
	[transactionScenario] [varchar](100) NULL,
	[transactionType] [varchar](100) NULL,
	[transactionMethod] [varchar](100) NULL,
	[transactionDeviceType] [varchar](100) NULL,
	[transactionDeviceId] [varchar](100) NULL,
	[transactionIPaddress] [varchar](100) NULL,
	[ipState] [varchar](100) NULL,
	[ipPostcode] [varchar](100) NULL,
	[ipCountryCode] [varchar](100) NULL,
	[isProxyIP] [varchar](100) NULL,
	[browserType] [varchar](100) NULL,
	[browserLanguage] [varchar](100) NULL,
	[paymentInstrumentType] [varchar](100) NULL,
	[cardType] [varchar](100) NULL,
	[cardNumberInputMethod] [varchar](100) NULL,
	[paymentInstrumentID] [varchar](100) NULL,
	[paymentBillingAddress] [varchar](100) NULL,
	[paymentBillingPostalCode] [varchar](100) NULL,
	[paymentBillingState] [varchar](100) NULL,
	[paymentBillingCountryCode] [varchar](100) NULL,
	[paymentBillingName] [varchar](100) NULL,
	[shippingAddress] [varchar](100) NULL,
	[shippingPostalCode] [varchar](100) NULL,
	[shippingCity] [varchar](100) NULL,
	[shippingState] [varchar](100) NULL,
	[shippingCountry] [varchar](100) NULL,
	[cvvVerifyResult] [varchar](100) NULL,
	[responseCode] [varchar](100) NULL,
	[digitalItemCount] [varchar](100) NULL,
	[physicalItemCount] [varchar](100) NULL,
	[purchaseProductType] [varchar](100) NULL,
	[transactionDateTime] [datetime] NULL,
	[accountOwnerName] [varchar](255) NULL,
	[accountAddress] [varchar](255) NULL,
	[accountPostalCode] [varchar](255) NULL,
	[accountCity] [varchar](255) NULL,
	[accountState] [varchar](255) NULL,
	[accountCountry] [varchar](255) NULL,
	[accountOpenDate] [varchar](255) NULL,
	[accountAge] [varchar](255) NULL,
	[isUserRegistered] [varchar](255) NULL,
	[paymentInstrumentAgeInAccount] [varchar](255) NULL,
	[numPaymentRejects1dPerUser] [varchar](255) NULL,
	[label] [int] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[Parsed_String_Acct_Processed]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[Parsed_String_Acct_Processed] as
select
label,
accountID,
transactionID,
transactionDateTime,
isnull(isProxyIP, '0') as isProxyIP, 
isnull(paymentInstrumentType, '0') as paymentInstrumentType,
isnull(cardType, '0') as cardType,
isnull(paymentBillingAddress, '0') as paymentBillingAddress,
isnull(paymentBillingPostalCode, '0') as paymentBillingPostalCode,
isnull(paymentBillingCountryCode, '0') as paymentBillingCountryCode,
isnull(paymentBillingName, '0') as paymentBillingName,
isnull(accountAddress, '0') as accountAddress,
isnull(accountPostalCode, '0') as accountPostalCode,
isnull(accountCountry, '0') as accountCountry,
isnull(accountOwnerName, '0') as accountOwnerName,
isnull(shippingAddress, '0') as shippingAddress,
isnull(transactionCurrencyCode, '0') as transactionCurrencyCode,
isnull(localHour,'-99') as localHour,
isnull(ipState, '0') as ipState,
isnull(ipPostCode, '0') as ipPostCode,
isnull(ipCountryCode, '0') as ipCountryCode,
isnull(browserLanguage, '0') as browserLanguage,
isnull(paymentBillingState, '0') as paymentBillingState,
isnull(accountState, '0') as accountState,
case when isnumeric(transactionAmountUSD)=1 then cast(transactionAmountUSD as float) else 0 end as transactionAmountUSD,
case when isnumeric(digitalItemCount)=1 then cast(digitalItemCount as float) else 0 end as digitalItemCount,
case when isnumeric(physicalItemCount)=1 then cast(physicalItemCount as float) else 0 end as physicalItemCount,
case when isnumeric(accountAge)=1 then cast(accountAge as float) else 0 end as accountAge,
case when isnumeric(paymentInstrumentAgeInAccount)=1 then cast(paymentInstrumentAgeInAccount as float) else 0 end as paymentInstrumentAgeInAccount,
case when isnumeric(numPaymentRejects1dPerUser)=1 then cast(numPaymentRejects1dPerUser as float) else 0 end as numPaymentRejects1dPerUser,
isUserRegistered = case when isUserRegistered like '%[0-9]%' then '0' else isUserRegistered end
from Parsed_String_Acct
where cast(transactionAmountUSD as float) >= 0 and   
      (case when transactionDateTime is null then 1 else 0 end) = 0 and
	  label < 2
GO
/****** Object:  View [dbo].[Parsed_String_Acct_Processed_Features1]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[Parsed_String_Acct_Processed_Features1] as
select t.label,t.accountID,t.transactionID,t.transactionDateTime,
t.transactionAmountUSD,
t.digitalItemCount,
t.physicalItemCount,
t.isProxyIP,
t.paymentInstrumentType,
t.cardType,
t.isUserRegistered,
t.accountAge,
t.paymentInstrumentAgeInAccount,
t.numPaymentRejects1dPerUser,
case when t.transactionAmountUSD > 150 then '1' else '0' end as isHighAmount,
case when t.paymentBillingAddress = t.accountAddress then '0' else '1' end as acctBillingAddressMismatchFlag,
case when t.paymentBillingPostalCode = t.accountPostalCode then '0' else '1' end as acctBillingPostalCodeMismatchFlag,
case when t.paymentBillingCountryCode = t.accountCountry then '0' else '1' end as acctBillingCountryMismatchFlag,
case when t.paymentBillingName = t.accountOwnerName then '0' else '1' end as acctBillingNameMismatchFlag,
case when t.shippingAddress = t.accountAddress then '0' else '1' end as acctShippingAddressMismatchFlag,
case when t.shippingAddress = t.paymentBillingAddress then '0' else '1' end as shippingBillingAddressMismatchFlag,
isnull(ac.risk,0) as accountCountryRisk,
isnull(apc.risk,0) as accountPostalCodeRisk,
isnull(actst.risk,0) as accountStateRisk,
isnull(bl.risk,0) as browserLanguageRisk,
isnull(ic.risk,0) as ipCountryCodeRisk,
isnull(ipc.risk,0) as ipPostCodeRisk,
isnull(ips.risk,0) as ipStateRisk,
isnull(lh.risk,0) as localHourRisk,
isnull(pbcc.risk,0) as paymentBillingCountryCodeRisk,
isnull(pbpc.risk,0) as paymentBillingPostalCodeRisk,
isnull(pbst.risk,0) as paymentBillingStateRisk,
isnull(tcc.risk,0) as transactionCurrencyCodeRisk
from Parsed_String_Acct_Processed as t
left join Risk_AccountCountry as ac on ac.accountCountry = t.accountCountry
left join Risk_AccountPostalCode as apc on apc.accountPostalCode = t.accountPostalCode
left join Risk_AccountState as actst on actst.accountState = t.accountState
left join Risk_BrowserLanguage as bl on bl.browserLanguage = t.browserLanguage
left join Risk_IpCountryCode as ic on ic.ipCountryCode = t.ipCountryCode
left join Risk_IpPostCode as ipc on ipc.ipPostCode = t.ipPostCode
left join Risk_IpState as ips on ips.ipState = t.ipState
left join Risk_LocalHour as lh on lh.localHour = t.localHour
left join Risk_PaymentBillingCountryCode as pbcc on pbcc.paymentBillingCountryCode = t.paymentBillingCountryCode
left join Risk_PaymentBillingPostalCode as pbpc on pbpc.paymentBillingPostalCode = t.paymentBillingPostalCode
left join Risk_PaymentBillingState as pbst on pbst.paymentBillingState = t.paymentBillingState
left join Risk_TransactionCurrencyCode as tcc on tcc.transactionCurrencyCode = t.transactionCurrencyCode
GO
/****** Object:  View [dbo].[Parsed_String_Acct_Processed_Features]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [dbo].[Parsed_String_Acct_Processed_Features] as
select * from Parsed_String_Acct_Processed_Features1 as t
outer apply
(select 
isnull(sum(case when t2.transactionDateTime > last24Hours then cast(t2.transactionAmountUSD as float) end),0) as sumPurchaseAmount1dPerUser,
isnull(count(case when t2.transactionDateTime > last24Hours then t2.transactionAmountUSD end),0) as sumPurchaseCount1dPerUser,
isnull(sum(cast(t2.transactionAmountUSD as float)),0) as sumPurchaseAmount30dPerUser,
isnull(count(t2.transactionAmountUSD),0) as sumPurchaseCount30dPerUser
from Transaction_History as t2
cross apply (values(t.transactionDateTime, DATEADD(hour, -24, t.transactionDateTime), DATEADD(day, -30, t.transactionDateTime))) as c(transactionDateTime, last24Hours, last30Days)
where t2.accountID = t.accountID and t2.transactionDateTime < t.transactionDateTime and t2.transactionDateTime > last30Days
) as a1
GO
/****** Object:  Table [dbo].[Account_Info]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Account_Info](
	[accountID] [varchar](255) NULL,
	[transactionDate] [varchar](255) NULL,
	[transactionTime] [varchar](255) NULL,
	[accountOwnerName] [varchar](255) NULL,
	[accountAddress] [varchar](255) NULL,
	[accountPostalCode] [varchar](255) NULL,
	[accountCity] [varchar](255) NULL,
	[accountState] [varchar](255) NULL,
	[accountCountry] [varchar](255) NULL,
	[accountOpenDate] [varchar](255) NULL,
	[accountAge] [varchar](255) NULL,
	[isUserRegistered] [varchar](255) NULL,
	[paymentInstrumentAgeInAccount] [varchar](255) NULL,
	[numPaymentRejects1dPerUser] [varchar](255) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Account_Info_Sort]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Account_Info_Sort](
	[accountID] [varchar](255) NULL,
	[transactionDate] [varchar](255) NULL,
	[transactionTime] [varchar](255) NULL,
	[accountOwnerName] [varchar](255) NULL,
	[accountAddress] [varchar](255) NULL,
	[accountPostalCode] [varchar](255) NULL,
	[accountCity] [varchar](255) NULL,
	[accountState] [varchar](255) NULL,
	[accountCountry] [varchar](255) NULL,
	[accountOpenDate] [varchar](255) NULL,
	[accountAge] [varchar](255) NULL,
	[isUserRegistered] [varchar](255) NULL,
	[paymentInstrumentAgeInAccount] [varchar](255) NULL,
	[numPaymentRejects1dPerUser] [varchar](255) NULL,
	[recordDateTime] [datetime] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Fraud]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Fraud](
	[transactionID] [varchar](255) NULL,
	[accountID] [varchar](255) NULL,
	[transactionAmount] [varchar](255) NULL,
	[transactionCurrencyCode] [varchar](255) NULL,
	[transactionDate] [varchar](255) NULL,
	[transactionTime] [varchar](255) NULL,
	[localHour] [varchar](255) NULL,
	[transactionDeviceId] [varchar](255) NULL,
	[transactionIPaddress] [varchar](255) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Hash_Id]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Hash_Id](
	[accountID] [varchar](255) NULL,
	[hashCode] [bigint] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Parsed_String]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Parsed_String](
	[transactionID] [varchar](100) NULL,
	[accountID] [varchar](100) NULL,
	[transactionAmountUSD] [varchar](100) NULL,
	[transactionAmount] [varchar](100) NULL,
	[transactionCurrencyCode] [varchar](100) NULL,
	[transactionCurrencyConversionRate] [varchar](100) NULL,
	[transactionDate] [varchar](100) NULL,
	[transactionTime] [varchar](100) NULL,
	[localHour] [varchar](100) NULL,
	[transactionScenario] [varchar](100) NULL,
	[transactionType] [varchar](100) NULL,
	[transactionMethod] [varchar](100) NULL,
	[transactionDeviceType] [varchar](100) NULL,
	[transactionDeviceId] [varchar](100) NULL,
	[transactionIPaddress] [varchar](100) NULL,
	[ipState] [varchar](100) NULL,
	[ipPostcode] [varchar](100) NULL,
	[ipCountryCode] [varchar](100) NULL,
	[isProxyIP] [varchar](100) NULL,
	[browserType] [varchar](100) NULL,
	[browserLanguage] [varchar](100) NULL,
	[paymentInstrumentType] [varchar](100) NULL,
	[cardType] [varchar](100) NULL,
	[cardNumberInputMethod] [varchar](100) NULL,
	[paymentInstrumentID] [varchar](100) NULL,
	[paymentBillingAddress] [varchar](100) NULL,
	[paymentBillingPostalCode] [varchar](100) NULL,
	[paymentBillingState] [varchar](100) NULL,
	[paymentBillingCountryCode] [varchar](100) NULL,
	[paymentBillingName] [varchar](100) NULL,
	[shippingAddress] [varchar](100) NULL,
	[shippingPostalCode] [varchar](100) NULL,
	[shippingCity] [varchar](100) NULL,
	[shippingState] [varchar](100) NULL,
	[shippingCountry] [varchar](100) NULL,
	[cvvVerifyResult] [varchar](100) NULL,
	[responseCode] [varchar](100) NULL,
	[digitalItemCount] [varchar](100) NULL,
	[physicalItemCount] [varchar](100) NULL,
	[purchaseProductType] [varchar](100) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Performance]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Performance](
	[ADR] [varchar](255) NULL,
	[PCT_NF_Acct] [varchar](255) NULL,
	[Dol_Frd] [varchar](255) NULL,
	[Do_NF] [varchar](255) NULL,
	[VDR] [varchar](255) NULL,
	[Acct_FP] [varchar](255) NULL,
	[PCT_Frd] [varchar](255) NULL,
	[PCT_NF] [varchar](255) NULL,
	[AFPR] [varchar](255) NULL,
	[TFPR] [varchar](255) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Performance_Auc]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Performance_Auc](
	[AUC] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Predict_Score]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Predict_Score](
	[accountID] [nvarchar](255) NULL,
	[transactionID] [nvarchar](288) NULL,
	[transactionDateTime] [nvarchar](255) NULL,
	[transactionAmountUSD] [float] NULL,
	[label] [int] NULL,
	[PredictedLabel] [nvarchar](255) NULL,
	[Score.1] [float] NULL,
	[Probability.1] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Predict_Score_Single_Transaction]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Predict_Score_Single_Transaction](
	[accountID] [nvarchar](255) NULL,
	[transactionID] [nvarchar](288) NULL,
	[transactionDateTime] [nvarchar](255) NULL,
	[transactionAmountUSD] [float] NULL,
	[label] [int] NULL,
	[PredictedLabel] [nvarchar](255) NULL,
	[Score.1] [float] NULL,
	[Probability.1] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Risk_Var]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Risk_Var](
	[ID] [int] NULL,
	[var_names] [varchar](255) NULL,
	[table_names] [varchar](255) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Tagged]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Tagged](
	[transactionID] [varchar](255) NULL,
	[accountID] [varchar](255) NULL,
	[transactionAmountUSD] [varchar](255) NULL,
	[transactionAmount] [varchar](255) NULL,
	[transactionCurrencyCode] [varchar](255) NULL,
	[transactionCurrencyConversionRate] [varchar](255) NULL,
	[transactionDate] [varchar](255) NULL,
	[transactionTime] [varchar](255) NULL,
	[localHour] [varchar](255) NULL,
	[transactionScenario] [varchar](255) NULL,
	[transactionType] [varchar](255) NULL,
	[transactionMethod] [varchar](255) NULL,
	[transactionDeviceType] [varchar](255) NULL,
	[transactionDeviceId] [varchar](255) NULL,
	[transactionIPaddress] [varchar](255) NULL,
	[ipState] [varchar](255) NULL,
	[ipPostcode] [varchar](255) NULL,
	[ipCountryCode] [varchar](255) NULL,
	[isProxyIP] [varchar](255) NULL,
	[browserType] [varchar](255) NULL,
	[browserLanguage] [varchar](255) NULL,
	[paymentInstrumentType] [varchar](255) NULL,
	[cardType] [varchar](255) NULL,
	[cardNumberInputMethod] [varchar](255) NULL,
	[paymentInstrumentID] [varchar](255) NULL,
	[paymentBillingAddress] [varchar](255) NULL,
	[paymentBillingPostalCode] [varchar](255) NULL,
	[paymentBillingState] [varchar](255) NULL,
	[paymentBillingCountryCode] [varchar](255) NULL,
	[paymentBillingName] [varchar](255) NULL,
	[shippingAddress] [varchar](255) NULL,
	[shippingPostalCode] [varchar](255) NULL,
	[shippingCity] [varchar](255) NULL,
	[shippingState] [varchar](255) NULL,
	[shippingCountry] [varchar](255) NULL,
	[cvvVerifyResult] [varchar](255) NULL,
	[responseCode] [varchar](255) NULL,
	[digitalItemCount] [varchar](255) NULL,
	[physicalItemCount] [varchar](255) NULL,
	[purchaseProductType] [varchar](255) NULL,
	[transactionDateTime] [datetime] NULL,
	[accountOwnerName] [varchar](255) NULL,
	[accountAddress] [varchar](255) NULL,
	[accountPostalCode] [varchar](255) NULL,
	[accountCity] [varchar](255) NULL,
	[accountState] [varchar](255) NULL,
	[accountCountry] [varchar](255) NULL,
	[accountOpenDate] [varchar](255) NULL,
	[accountAge] [varchar](255) NULL,
	[isUserRegistered] [varchar](255) NULL,
	[paymentInstrumentAgeInAccount] [varchar](255) NULL,
	[numPaymentRejects1dPerUser] [varchar](255) NULL,
	[tDT] [datetime] NULL,
	[sDT] [datetime] NULL,
	[eDT] [datetime] NULL,
	[label] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Tagged_Testing]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Tagged_Testing](
	[transactionID] [varchar](255) NULL,
	[accountID] [varchar](255) NULL,
	[transactionAmountUSD] [varchar](255) NULL,
	[transactionAmount] [varchar](255) NULL,
	[transactionCurrencyCode] [varchar](255) NULL,
	[transactionCurrencyConversionRate] [varchar](255) NULL,
	[transactionDate] [varchar](255) NULL,
	[transactionTime] [varchar](255) NULL,
	[localHour] [varchar](255) NULL,
	[transactionScenario] [varchar](255) NULL,
	[transactionType] [varchar](255) NULL,
	[transactionMethod] [varchar](255) NULL,
	[transactionDeviceType] [varchar](255) NULL,
	[transactionDeviceId] [varchar](255) NULL,
	[transactionIPaddress] [varchar](255) NULL,
	[ipState] [varchar](255) NULL,
	[ipPostcode] [varchar](255) NULL,
	[ipCountryCode] [varchar](255) NULL,
	[isProxyIP] [varchar](255) NULL,
	[browserType] [varchar](255) NULL,
	[browserLanguage] [varchar](255) NULL,
	[paymentInstrumentType] [varchar](255) NULL,
	[cardType] [varchar](255) NULL,
	[cardNumberInputMethod] [varchar](255) NULL,
	[paymentInstrumentID] [varchar](255) NULL,
	[paymentBillingAddress] [varchar](255) NULL,
	[paymentBillingPostalCode] [varchar](255) NULL,
	[paymentBillingState] [varchar](255) NULL,
	[paymentBillingCountryCode] [varchar](255) NULL,
	[paymentBillingName] [varchar](255) NULL,
	[shippingAddress] [varchar](255) NULL,
	[shippingPostalCode] [varchar](255) NULL,
	[shippingCity] [varchar](255) NULL,
	[shippingState] [varchar](255) NULL,
	[shippingCountry] [varchar](255) NULL,
	[cvvVerifyResult] [varchar](255) NULL,
	[responseCode] [varchar](255) NULL,
	[digitalItemCount] [varchar](255) NULL,
	[physicalItemCount] [varchar](255) NULL,
	[purchaseProductType] [varchar](255) NULL,
	[transactionDateTime] [datetime] NULL,
	[accountOwnerName] [varchar](255) NULL,
	[accountAddress] [varchar](255) NULL,
	[accountPostalCode] [varchar](255) NULL,
	[accountCity] [varchar](255) NULL,
	[accountState] [varchar](255) NULL,
	[accountCountry] [varchar](255) NULL,
	[accountOpenDate] [varchar](255) NULL,
	[accountAge] [varchar](255) NULL,
	[isUserRegistered] [varchar](255) NULL,
	[paymentInstrumentAgeInAccount] [varchar](255) NULL,
	[numPaymentRejects1dPerUser] [varchar](255) NULL,
	[tDT] [datetime] NULL,
	[sDT] [datetime] NULL,
	[eDT] [datetime] NULL,
	[label] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Trained_Model]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Trained_Model](
	[id] [varchar](200) NOT NULL,
	[value] [varbinary](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Untagged_Transactions]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Untagged_Transactions](
	[transactionID] [varchar](255) NULL,
	[accountID] [varchar](255) NULL,
	[transactionAmountUSD] [varchar](255) NULL,
	[transactionAmount] [varchar](255) NULL,
	[transactionCurrencyCode] [varchar](255) NULL,
	[transactionCurrencyConversionRate] [varchar](255) NULL,
	[transactionDate] [varchar](255) NULL,
	[transactionTime] [varchar](255) NULL,
	[localHour] [varchar](255) NULL,
	[transactionScenario] [varchar](255) NULL,
	[transactionType] [varchar](255) NULL,
	[transactionMethod] [varchar](255) NULL,
	[transactionDeviceType] [varchar](255) NULL,
	[transactionDeviceId] [varchar](255) NULL,
	[transactionIPaddress] [varchar](255) NULL,
	[ipState] [varchar](255) NULL,
	[ipPostcode] [varchar](255) NULL,
	[ipCountryCode] [varchar](255) NULL,
	[isProxyIP] [varchar](255) NULL,
	[browserType] [varchar](255) NULL,
	[browserLanguage] [varchar](255) NULL,
	[paymentInstrumentType] [varchar](255) NULL,
	[cardType] [varchar](255) NULL,
	[cardNumberInputMethod] [varchar](255) NULL,
	[paymentInstrumentID] [varchar](255) NULL,
	[paymentBillingAddress] [varchar](255) NULL,
	[paymentBillingPostalCode] [varchar](255) NULL,
	[paymentBillingState] [varchar](255) NULL,
	[paymentBillingCountryCode] [varchar](255) NULL,
	[paymentBillingName] [varchar](255) NULL,
	[shippingAddress] [varchar](255) NULL,
	[shippingPostalCode] [varchar](255) NULL,
	[shippingCity] [varchar](255) NULL,
	[shippingState] [varchar](255) NULL,
	[shippingCountry] [varchar](255) NULL,
	[cvvVerifyResult] [varchar](255) NULL,
	[responseCode] [varchar](255) NULL,
	[digitalItemCount] [varchar](255) NULL,
	[physicalItemCount] [varchar](255) NULL,
	[purchaseProductType] [varchar](255) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Untagged_Transactions_Acct]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Untagged_Transactions_Acct](
	[transactionID] [varchar](255) NULL,
	[accountID] [varchar](255) NULL,
	[transactionAmountUSD] [varchar](255) NULL,
	[transactionAmount] [varchar](255) NULL,
	[transactionCurrencyCode] [varchar](255) NULL,
	[transactionCurrencyConversionRate] [varchar](255) NULL,
	[transactionDate] [varchar](255) NULL,
	[transactionTime] [varchar](255) NULL,
	[localHour] [varchar](255) NULL,
	[transactionScenario] [varchar](255) NULL,
	[transactionType] [varchar](255) NULL,
	[transactionMethod] [varchar](255) NULL,
	[transactionDeviceType] [varchar](255) NULL,
	[transactionDeviceId] [varchar](255) NULL,
	[transactionIPaddress] [varchar](255) NULL,
	[ipState] [varchar](255) NULL,
	[ipPostcode] [varchar](255) NULL,
	[ipCountryCode] [varchar](255) NULL,
	[isProxyIP] [varchar](255) NULL,
	[browserType] [varchar](255) NULL,
	[browserLanguage] [varchar](255) NULL,
	[paymentInstrumentType] [varchar](255) NULL,
	[cardType] [varchar](255) NULL,
	[cardNumberInputMethod] [varchar](255) NULL,
	[paymentInstrumentID] [varchar](255) NULL,
	[paymentBillingAddress] [varchar](255) NULL,
	[paymentBillingPostalCode] [varchar](255) NULL,
	[paymentBillingState] [varchar](255) NULL,
	[paymentBillingCountryCode] [varchar](255) NULL,
	[paymentBillingName] [varchar](255) NULL,
	[shippingAddress] [varchar](255) NULL,
	[shippingPostalCode] [varchar](255) NULL,
	[shippingCity] [varchar](255) NULL,
	[shippingState] [varchar](255) NULL,
	[shippingCountry] [varchar](255) NULL,
	[cvvVerifyResult] [varchar](255) NULL,
	[responseCode] [varchar](255) NULL,
	[digitalItemCount] [varchar](255) NULL,
	[physicalItemCount] [varchar](255) NULL,
	[purchaseProductType] [varchar](255) NULL,
	[transactionDateTime] [datetime] NULL,
	[accountOwnerName] [varchar](255) NULL,
	[accountAddress] [varchar](255) NULL,
	[accountPostalCode] [varchar](255) NULL,
	[accountCity] [varchar](255) NULL,
	[accountState] [varchar](255) NULL,
	[accountCountry] [varchar](255) NULL,
	[accountOpenDate] [varchar](255) NULL,
	[accountAge] [varchar](255) NULL,
	[isUserRegistered] [varchar](255) NULL,
	[paymentInstrumentAgeInAccount] [varchar](255) NULL,
	[numPaymentRejects1dPerUser] [varchar](255) NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[Parsed_String_Acct] ADD  DEFAULT ((-1)) FOR [label]
GO
/****** Object:  StoredProcedure [dbo].[CreateRiskTable]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[CreateRiskTable] 
@name varchar(max),
@table_name varchar(max)
as
begin
declare @filltablesql nvarchar(max)
declare @droptablesql nvarchar(max)
declare @removenullconstrain nvarchar(max)
declare @addprimarykey nvarchar(max)

/* drop corresponding table if it already exists */
set @droptablesql = 'DROP TABLE IF EXISTS ' + @table_name
exec sp_executesql @droptablesql

/* create risk table */
set @filltablesql = 'select ' + @name + ' , log(odds/(1-odds)) as risk 
            into .dbo.' + @table_name + 
			' from (select distinct ' + @name + ' ,cast((sum(label)+10) as float)/cast((sum(label)+sum(1-label)+100) as float) as odds 
			from Tagged_Training_Processed group by ' + @name + ' ) temp'

/* example: when @name=localHour, @table_name=Risk_LocalHour, @sql is the following:
select localHour , log(odds/(1-odds)) as risk 
            into Risk_LocalHour from (select distinct localHour ,cast((sum(label)+10) as float)/cast((sum(label)+sum(1-label)+100) as float) as odds 
			from Tagged_Training group by localHour ) temp
*/

exec sp_executesql @filltablesql
end
GO
/****** Object:  StoredProcedure [dbo].[CreateRiskTable_ForAll]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[CreateRiskTable_ForAll]
as
begin

/* create a table to store names of variables and risk tables. will be used as reference in the loop later */ 
if exists 
(select * from sysobjects where name like 'Risk_Var') 
truncate table Risk_Var
else
create table dbo.Risk_Var (ID int,var_names varchar(255), table_names varchar(255));

insert into Risk_Var values (1, 'transactionCurrencyCode', 'Risk_TransactionCurrencyCode');
insert into Risk_Var values (2, 'localHour', 'Risk_LocalHour');
insert into Risk_Var values (3, 'ipState', 'Risk_IpState');
insert into Risk_Var values (4, 'ipPostCode', 'Risk_IpPostCode');
insert into Risk_Var values (5, 'ipCountryCode', 'Risk_IpCountryCode');
insert into Risk_Var values (6, 'browserLanguage', 'Risk_BrowserLanguage');
insert into Risk_Var values (7, 'paymentBillingPostalCode', 'Risk_PaymentBillingPostalCode');
insert into Risk_Var values (8, 'paymentBillingState', 'Risk_PaymentBillingState');
insert into Risk_Var values (9, 'paymentBillingCountryCode', 'Risk_PaymentBillingCountryCode');
insert into Risk_Var values (10, 'accountPostalCode', 'Risk_AccountPostalCode');
insert into Risk_Var values (11, 'accountState', 'Risk_AccountState');
insert into Risk_Var values (12, 'accountCountry', 'Risk_AccountCountry');

/* create all risk tables by looping over all variables in reference table and executing CreateRiskTable stored procedure */
DECLARE @name_1 NVARCHAR(100)
DECLARE @name_2 NVARCHAR(100)
DECLARE @getname CURSOR

SET @getname = CURSOR FOR
SELECT var_names,
	   table_names
FROM   Risk_Var
OPEN @getname
FETCH NEXT
FROM @getname INTO @name_1,@name_2
WHILE @@FETCH_STATUS = 0
BEGIN
    EXEC CreateRiskTable @name_1,@name_2 -- create risk table by calling stored procedure CreateRiskTable
    FETCH NEXT
    FROM @getname INTO @name_1, @name_2
END

CLOSE @getname
DEALLOCATE @getname
end
GO
/****** Object:  StoredProcedure [dbo].[EvaluateR]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[EvaluateR] @table nvarchar(max)
as
begin

/* create table to store the result */
if exists 
(select * from sysobjects where name like 'Performance') 
truncate table Performance
else
create table Performance ( 
ADR varchar(255),
PCT_NF_Acct varchar(255),
Dol_Frd varchar(255),
Do_NF varchar(255),
VDR varchar(255),
Acct_FP varchar(255),
PCT_Frd varchar(255),
PCT_NF varchar(255),
AFPR varchar(255),
TFPR varchar(255)
);

/* specify the query to select data to be evaluated. this query will be used as input for following R script */
declare @GetScoreData nvarchar(max) 
set @GetScoreData =  'select accountID, transactionDateTime, transactionAmountUSD, label, [Probability.1] from ' + @table + ' order by accountID, transactionDateTime'

/* R script to generate account level metrics */
insert into Performance
exec sp_execute_external_script @language = N'R',
                                  @script = N'
####################################################################################################
## Fraud account level metrics
####################################################################################################
# Implement account-level performance metrics and transaction-level metrics.
# ADR -- Fraud account detection rate
# VDR -- Value detection rate. The percentage of values saved.
# AFPR -- Account-level false positive ratio.
# ROC  -- Transaction-level ROC 
# $ROC -- Dollar weighted ROC
# TFPR -- Transaction level false positive ratio.
# sampling rate are taken into consideration to derive performance on original unsampled dataset.
# contactPeriod is in the unit of days, indicating the lag before a customer is contacted again 
# to verify high-score transactions are legitimate. 
scr2stat <-function(dataset, contactPeriod, sampleRateNF,sampleRateFrd)
 {
  #scr quantization/binning into 1000 equal bins
  
  #accout level score is the maximum of trans scores of that account
  #all transactions after the first fraud transaction detected are value savings
  #input score file needs to be acct-date-time sorted   
  dataset$"Scored Probabilities" <- dataset$Probability.1
  
  fields = names(dataset)
  if(! ("accountID" %in% fields)) 
  {print ("Error: Need accountID column!")}
  if(! ("transactionDateTime" %in% fields)) 
  {print ("Error: Need transactionDateTime column!")}
  if(! ("transactionAmountUSD" %in% fields))
  {print ("Error: Need transactionAmountUSD column!")}
  if(! ("Scored Probabilities" %in% fields))
  {print ("Error: Need Scored Probabilities column!")}
  
  nRows = dim(dataset)[1];
  
  nBins = 1000; 
  
  #1. first calculate the perf stats by score band  
  
  prev_acct =dataset$accountID[1]
  prev_score = 0
  is_frd_acct = 0
  max_scr = 0	
  
  
  scr_hash=matrix(0, nBins,10)	
  
  f_scr_rec = vector("numeric",nBins)
  #nf_scr_rec = matrix(0, nBins,2)  #count, datetime
  nf_scr_rec_count = vector("numeric",nBins)
  nf_scr_rec_time = vector("numeric",nBins)

  
  for (r in 1:nRows)


  {
    acct = as.character(dataset$accountID[r])
    dolamt = as.double(dataset$transactionAmountUSD[r])
    label = dataset$label[r]
    score = dataset$"Scored Probabilities"[r]
    datetime = dataset$transactionDateTime[r]
    
    if(score == 0)
    { 
      score = score + 0.00001
      print ("The following account has zero score!")
      print (paste(acct,dolamt,datetime,sep=" "));
    }
    
    if(label == 2) next
    
    
    if (acct != prev_acct){
      scr_bin = ceiling(max_scr*nBins)
      
      
      if (is_frd_acct) {
        scr_hash[,5] = 	scr_hash[,5] + f_scr_rec   #vdr
        scr_hash[scr_bin,1] = scr_hash[scr_bin,1] + 1   #adr
      }
      else {
        scr_hash[,6] =  scr_hash[,6] + as.numeric(nf_scr_rec_count)  #FP with contact period, a FP could be considered as multiple
        scr_hash[scr_bin,2] = scr_hash[scr_bin,2]+1;   #a FP account considered one acct  		
      }
      
      f_scr_rec = vector("numeric",nBins)
      
      nf_scr_rec_count = vector("numeric",nBins)
      nf_scr_rec_time = vector("numeric",nBins)
      
      is_frd_acct = 0;
      total_nf_dol = 0;
      total_frd_dol = 0;
      max_scr = 0;
    }
    
    if (score > max_scr) {
      max_scr = score;
    }
    
    #find out the bin the current acct falls in. 
    tran_scr_bin = ceiling(score*nBins)
    
    
    #dollar weighted ROC and regular ROC
    if(label == 1){
      scr_hash[tran_scr_bin,3] = scr_hash[tran_scr_bin,3]+dolamt;
      scr_hash[tran_scr_bin,7] = scr_hash[tran_scr_bin,7]+1;
      is_frd_acct = 1;
    }
    else{
      scr_hash[tran_scr_bin,4] = scr_hash[tran_scr_bin,4]+dolamt;		
      scr_hash[tran_scr_bin,8] = scr_hash[tran_scr_bin,8]+1;  	
    }
    
    #ADR/VDR
    if(label == 1)
    {
      #ADR
      f_scr_rec[tran_scr_bin] = 1
      
      #VDR
      #If a higher score appeared before the current score, then this is also savings for the higher score.
      #Once a fraud transaction is discovered, all subsequent approved transactons are savings.
      for(i in  1: ceiling(max_scr*nBins))
      {
        f_scr_rec[i] = f_scr_rec[i] + dolamt
      }
    }
    else
    { 
      #False Positive Accounts (FP) with recontact period
      #check if there is any earlier dates for the same or lower score
      #update the count and dates when within recontact period
      
      #for(i in  1: floor(max_scr*nBins))
      for(i in  1: tran_scr_bin)
      {
        
        prev_time = nf_scr_rec_time[i]
        #print(paste(i, tran_scr_bin, sep=" "))
        #print(paste(acct, datetime, sep=" "))
        #print(prev_time)
        if( prev_time > 0)
        {
		  timeDiff = difftime(strptime(datetime,"%Y-%m-%d %H:%M:%S"),strptime(prev_time,"%Y-%m-%d %H:%M:%S"), units="days" ) 
          if(timeDiff >= contactPeriod)
          {
            nf_scr_rec_count[i] = nf_scr_rec_count[i] +1
            nf_scr_rec_time[i] = datetime
          }
        }
        else
        {
          nf_scr_rec_count[i] = nf_scr_rec_count[i] +1
          nf_scr_rec_time[i] = datetime
        }
        
      }
      
    }  
    
    prev_acct = acct;
    
  }
  
  
  #1 -- #Frd Acct
  #2 -- #NF  Acct with infinite recontact period
  #3 -- $Frd Tran
  #4 -- $NF  Tran
  #5 -- $Frd Saving
  #6 -- #NF Acct with finite recontact period
  #7 -- #Frd Tran
  #8 -- #NF Tran
  #9 -- AFPR
  #10 --TFPR
  
  #2. now calculate the cumulative perf counts
  
  # 5, 6 already in cumulative during previous calculation
  
  for (i in (nBins-1):1){
    
    for(j in c(1:4,7:8)){
      scr_hash[i,j] = scr_hash[i,j]+scr_hash[i+1,j];
    }
  }
  
  #3 calculate AFPR, TFPR:
  scr_hash[,9] = scr_hash[,6]/(scr_hash[,1]+0.0001)
  scr_hash[,10] = scr_hash[,8]/(scr_hash[,7]+0.0001)
  
  #print(scr_hash)
  
  #4. now calculate the ADR/VDR, ROC percentage	 	
  for(j in c(1:5,7:8)){
    scr_hash[,j] = scr_hash[,j]/scr_hash[1,j];
  }
  
  #5. Adjust for sampling rate
  for (j in c(1, 3, 5 ,7))
  {
    scr_hash[,j]= scr_hash[,j]/sampleRateFrd
  }
  
  for (j in c(2, 4, 6 ,8))
  {
    scr_hash[,j]= scr_hash[,j]/sampleRateNF
  }
  
  for (j in c(9, 10))
  {
    scr_hash[,j]= scr_hash[,j]/sampleRateNF*sampleRateFrd
  }
  
  
  perf.df = as.data.frame(scr_hash)
  colnames(perf.df) = c(''ADR'',''PCT NF Acct'',''Dol Frd'', ''Dol NF'', ''VDR'', ''Acct FP(recontact period)'', ''PCT Frd'', ''PCT NF'',''AFPR'',''TFPR'')
  return (perf.df)	
 }
 scored_data <- InputDataSet
 scored_data$transactionDateTime <- as.character(scored_data$transactionDateTime)
 perf <- scr2stat(scored_data,contactPeriod=30, sampleRateNF=1,sampleRateFrd=1)
 OutputDataSet <- as.data.frame(perf)
',
  @input_data_1 = @GetScoreData
;
end
GO
/****** Object:  StoredProcedure [dbo].[EvaluateR_auc]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[EvaluateR_auc] @table nvarchar(max)
as
begin

/* create table to store AUC value */
if exists 
(select * from sysobjects where name like 'Performance_Auc') 
truncate table Performance_Auc
else
create table Performance_Auc ( 
AUC float
);

/* specify the query to select data to be evaluated. this query will be used as input for following R script */
declare @GetScoreData nvarchar(max) 
set @GetScoreData =  'select * from ' + @table

/* R script to calculate AUC */
insert into Performance_Auc
exec sp_execute_external_script @language = N'R',
                                  @script = N'
 Predictions <- InputDataSet
 Predictions$label <- as.numeric(as.character(Predictions$label))

 # Compute the AUC. 
 ROC <- rxRoc(actualVarName = "label", predVarNames = "Probability.1", data = Predictions, numBreaks = 1000)
 AUC <- rxAuc(ROC)
 OutputDataSet <- as.data.frame(AUC)
',
  @input_data_1 = @GetScoreData
;
end
GO
/****** Object:  StoredProcedure [dbo].[FeatureEngineer]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[FeatureEngineer] @table nvarchar(max)
as
begin

/* create mismatch flags and assign risk values */ 
declare 
@sql_dropview1 nvarchar(max) = '';
set @sql_dropview1 = '
DROP VIEW IF EXISTS ' + @table + '_Features1'
exec sp_executesql @sql_dropview1;

declare @sql_fe1 nvarchar(max) = '';
set @sql_fe1 = 'create view ' + @table + '_Features1 as
select t.label,t.accountID,t.transactionID,t.transactionDateTime,
t.transactionAmountUSD,
t.digitalItemCount,
t.physicalItemCount,
t.isProxyIP,
t.paymentInstrumentType,
t.cardType,
t.isUserRegistered,
t.accountAge,
t.paymentInstrumentAgeInAccount,
t.numPaymentRejects1dPerUser,
case when t.transactionAmountUSD > 150 then ''1'' else ''0'' end as isHighAmount,
case when t.paymentBillingAddress = t.accountAddress then ''0'' else ''1'' end as acctBillingAddressMismatchFlag,
case when t.paymentBillingPostalCode = t.accountPostalCode then ''0'' else ''1'' end as acctBillingPostalCodeMismatchFlag,
case when t.paymentBillingCountryCode = t.accountCountry then ''0'' else ''1'' end as acctBillingCountryMismatchFlag,
case when t.paymentBillingName = t.accountOwnerName then ''0'' else ''1'' end as acctBillingNameMismatchFlag,
case when t.shippingAddress = t.accountAddress then ''0'' else ''1'' end as acctShippingAddressMismatchFlag,
case when t.shippingAddress = t.paymentBillingAddress then ''0'' else ''1'' end as shippingBillingAddressMismatchFlag,
isnull(ac.risk,0) as accountCountryRisk,
isnull(apc.risk,0) as accountPostalCodeRisk,
isnull(actst.risk,0) as accountStateRisk,
isnull(bl.risk,0) as browserLanguageRisk,
isnull(ic.risk,0) as ipCountryCodeRisk,
isnull(ipc.risk,0) as ipPostCodeRisk,
isnull(ips.risk,0) as ipStateRisk,
isnull(lh.risk,0) as localHourRisk,
isnull(pbcc.risk,0) as paymentBillingCountryCodeRisk,
isnull(pbpc.risk,0) as paymentBillingPostalCodeRisk,
isnull(pbst.risk,0) as paymentBillingStateRisk,
isnull(tcc.risk,0) as transactionCurrencyCodeRisk
from ' +@table + ' as t
left join Risk_AccountCountry as ac on ac.accountCountry = t.accountCountry
left join Risk_AccountPostalCode as apc on apc.accountPostalCode = t.accountPostalCode
left join Risk_AccountState as actst on actst.accountState = t.accountState
left join Risk_BrowserLanguage as bl on bl.browserLanguage = t.browserLanguage
left join Risk_IpCountryCode as ic on ic.ipCountryCode = t.ipCountryCode
left join Risk_IpPostCode as ipc on ipc.ipPostCode = t.ipPostCode
left join Risk_IpState as ips on ips.ipState = t.ipState
left join Risk_LocalHour as lh on lh.localHour = t.localHour
left join Risk_PaymentBillingCountryCode as pbcc on pbcc.paymentBillingCountryCode = t.paymentBillingCountryCode
left join Risk_PaymentBillingPostalCode as pbpc on pbpc.paymentBillingPostalCode = t.paymentBillingPostalCode
left join Risk_PaymentBillingState as pbst on pbst.paymentBillingState = t.paymentBillingState
left join Risk_TransactionCurrencyCode as tcc on tcc.transactionCurrencyCode = t.transactionCurrencyCode
'
exec sp_executesql @sql_fe1;

/* create aggregates on the fly */
declare 
@sql_dropview nvarchar(max) = '';
set @sql_dropview = '
DROP VIEW IF EXISTS ' + @table + '_Features'
exec sp_executesql @sql_dropview;

declare @sql_fe nvarchar(max) = '';
set @sql_fe = 'create view ' + @table + '_Features as
select * from ' + @table + '_Features1 as t
outer apply
(select 
isnull(sum(case when t2.transactionDateTime > last24Hours then cast(t2.transactionAmountUSD as float) end),0) as sumPurchaseAmount1dPerUser,
isnull(count(case when t2.transactionDateTime > last24Hours then t2.transactionAmountUSD end),0) as sumPurchaseCount1dPerUser,
isnull(sum(cast(t2.transactionAmountUSD as float)),0) as sumPurchaseAmount30dPerUser,
isnull(count(t2.transactionAmountUSD),0) as sumPurchaseCount30dPerUser
from Transaction_History as t2
cross apply (values(t.transactionDateTime, DATEADD(hour, -24, t.transactionDateTime), DATEADD(day, -30, t.transactionDateTime))) as c(transactionDateTime, last24Hours, last30Days)
where t2.accountID = t.accountID and t2.transactionDateTime < t.transactionDateTime and t2.transactionDateTime > last30Days
) as a1'

exec sp_executesql @sql_fe;
end




GO
/****** Object:  StoredProcedure [dbo].[MergeAcctInfo]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[MergeAcctInfo] @table nvarchar(max)
as
begin

declare @droptable nvarchar(max) 
set @droptable = 'drop table if exists ' + @table + '_Acct'
exec sp_executesql @droptable

/* Merge with AccountInfo_Sort table */
declare @MergeQuery nvarchar(max) 
set @MergeQuery =  
'
select t1.*,
       t2.accountOwnerName,
	   t2.accountAddress,
	   t2.accountPostalCode,
	   t2.accountCity,
	   t2.accountState,
	   t2.accountCountry,
	   t2.accountOpenDate,
	   t2.accountAge,
	   t2.isUserRegistered,
	   t2.paymentInstrumentAgeInAccount,
	   t2.numPaymentRejects1dPerUser
into ' + @table + '_Acct ' +
'from 
 (select *, 
       convert(datetime,stuff(stuff(stuff(concat(transactionDate,dbo.FormatTime(transactionTime)), 9, 0, '' ''), 12, 0, '':''), 15, 0, '':'')) as transactionDateTime
  from ' + @table + ') as t1
 outer apply 
 (select top 1 * -- the top 1 is the maximum transactionDateTime up to current transactionDateTime
  from Account_Info_Sort as t
  where t.accountID = t1.accountID and t.recordDateTime <= t1.transactionDateTime) as t2
where t1.accountID = t2.accountID
'

exec sp_executesql @MergeQuery
end
GO
/****** Object:  StoredProcedure [dbo].[ParseStr]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[ParseStr] @inputstring VARCHAR(MAX)
as
begin

/* Reformat the long string into XML format whose elements can be retrieved by location index */
declare @parsequery nvarchar(max)
set @parsequery = '
DECLARE @tmp table ( ID int Identity(1,1)  ,[Name] nvarchar(max))
INSERT into @tmp SELECT ''' + @inputstring + '''
drop table if exists Parsed_String
;WITH tmp AS
( 
    SELECT
        CAST(''<M>'' + REPLACE([Name], '','' , ''</M><M>'') + ''</M>'' AS XML) 
        AS [NameParsed]
    FROM  @tmp 
)
SELECT
     case when [NameParsed].value(''/M[1]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[1]'', ''varchar (100)'') end As [transactionID],
     case when [NameParsed].value(''/M[2]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[2]'', ''varchar (100)'') end As [accountID],
     case when [NameParsed].value(''/M[3]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[3]'', ''varchar (100)'') end As [transactionAmountUSD],
	 case when [NameParsed].value(''/M[4]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[4]'', ''varchar (100)'') end As transactionAmount,
     case when [NameParsed].value(''/M[5]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[5]'', ''varchar (100)'') end As [transactionCurrencyCode],
     case when [NameParsed].value(''/M[6]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[6]'', ''varchar (100)'') end As [transactionCurrencyConversionRate],
	 case when [NameParsed].value(''/M[7]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[7]'', ''varchar (100)'') end As [transactionDate],
     case when [NameParsed].value(''/M[8]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[8]'', ''varchar (100)'') end As [transactionTime],
     case when [NameParsed].value(''/M[9]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[9]'', ''varchar (100)'') end As [localHour],
	 case when [NameParsed].value(''/M[10]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[10]'', ''varchar (100)'') end As [transactionScenario],
     case when [NameParsed].value(''/M[11]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[11]'', ''varchar (100)'') end As [transactionType],
     case when [NameParsed].value(''/M[12]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[12]'', ''varchar (100)'') end As [transactionMethod],
	 case when [NameParsed].value(''/M[13]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[13]'', ''varchar (100)'') end As [transactionDeviceType],
     case when [NameParsed].value(''/M[14]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[14]'', ''varchar (100)'') end As [transactionDeviceId],
     case when [NameParsed].value(''/M[15]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[15]'', ''varchar (100)'') end As [transactionIPaddress],
	 case when [NameParsed].value(''/M[16]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[16]'', ''varchar (100)'') end As [ipState],     
	 case when [NameParsed].value(''/M[17]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[17]'', ''varchar (100)'') end As [ipPostcode],
     case when [NameParsed].value(''/M[18]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[18]'', ''varchar (100)'') end As [ipCountryCode],
     case when [NameParsed].value(''/M[19]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[19]'', ''varchar (100)'') end As [isProxyIP],
	 case when [NameParsed].value(''/M[20]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[20]'', ''varchar (100)'') end As [browserType],
     case when [NameParsed].value(''/M[21]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[21]'', ''varchar (100)'') end As [browserLanguage],
     case when [NameParsed].value(''/M[22]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[22]'', ''varchar (100)'') end As [paymentInstrumentType],
	 case when [NameParsed].value(''/M[23]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[23]'', ''varchar (100)'') end As [cardType],
	 case when [NameParsed].value(''/M[24]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[24]'', ''varchar (100)'') end As [cardNumberInputMethod],
     case when [NameParsed].value(''/M[25]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[25]'', ''varchar (100)'') end As [paymentInstrumentID],
     case when [NameParsed].value(''/M[26]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[26]'', ''varchar (100)'') end As [paymentBillingAddress],
	 case when [NameParsed].value(''/M[27]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[27]'', ''varchar (100)'') end As [paymentBillingPostalCode],
     case when [NameParsed].value(''/M[28]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[28]'', ''varchar (100)'') end As [paymentBillingState],
     case when [NameParsed].value(''/M[29]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[29]'', ''varchar (100)'') end As [paymentBillingCountryCode],
	 case when [NameParsed].value(''/M[30]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[30]'', ''varchar (100)'') end As [paymentBillingName],
     case when [NameParsed].value(''/M[31]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[31]'', ''varchar (100)'') end As [shippingAddress],
     case when [NameParsed].value(''/M[32]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[32]'', ''varchar (100)'') end As [shippingPostalCode],
	 case when [NameParsed].value(''/M[33]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[33]'', ''varchar (100)'') end As [shippingCity],
	 case when [NameParsed].value(''/M[34]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[34]'', ''varchar (100)'') end As [shippingState],
     case when [NameParsed].value(''/M[35]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[35]'', ''varchar (100)'') end As [shippingCountry],
     case when [NameParsed].value(''/M[36]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[36]'', ''varchar (100)'') end As [cvvVerifyResult],
	 case when [NameParsed].value(''/M[37]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[37]'', ''varchar (100)'') end As [responseCode],
     case when [NameParsed].value(''/M[38]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[38]'', ''varchar (100)'') end As [digitalItemCount],
     case when [NameParsed].value(''/M[39]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[39]'', ''varchar (100)'') end As [physicalItemCount],
	 case when [NameParsed].value(''/M[40]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[40]'', ''varchar (100)'') end As [purchaseProductType]
into Parsed_String  
FROM tmp'
exec sp_executesql @parsequery
end
GO
/****** Object:  StoredProcedure [dbo].[PredictR]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[PredictR] @inputtable nvarchar(max),
                          @outputtable nvarchar(max),
                          @getacctflag nvarchar(max)
as
begin

/* merge with accountInfo table if getacctflag = '1' */
declare @mergeacct nvarchar(max) = '';
set @mergeacct = 'if cast(' + @getacctflag + ' as int) = 1 
begin
 EXEC MergeAcctInfo ' + @inputtable + '
end'
exec sp_executesql @mergeacct

/* select @inputtable into @table_acct if getacctflag = '0' */
declare @renametable nvarchar(max) = '';
set @renametable = 
'if cast(' + @getacctflag + ' as int) = 0 
begin
  drop table if exists ' + @inputtable + '_Acct
  select * into ' + @inputtable + '_Acct from ' + @inputtable + '
end'
exec sp_executesql @renametable

/* add a fake label if label doesn't exist */
declare @addlabel nvarchar(max) = '';
set @addlabel = '
IF NOT EXISTS(SELECT 1 FROM sys.columns 
          WHERE Name = N''label''
          AND Object_ID = Object_ID(N''' + @inputtable + '_Acct''))
BEGIN
    alter table ' + @inputtable + '_Acct add label int not null default(-1)
END'
exec sp_executesql @addlabel

/* preprocessing by calling the stored procedure 'Preprocess' */
declare @preprocess nvarchar(max)
set @preprocess = 'exec Preprocess ' + @inputtable + '_Acct'
exec sp_executesql @preprocess

/* save transactions to history table */
declare @sql_save2history nvarchar(max)
set @sql_save2history = 'exec Save2TransactionHistory ' + @inputtable + '_Acct_Processed, ''0'''
exec sp_executesql @sql_save2history

/* feature engineering by calling the stored procedure 'FeatureEngineer' */
declare @fe_query nvarchar(max) 
set @fe_query = 'exec FeatureEngineer ' + @inputtable + '_Acct_Processed'
exec sp_executesql @fe_query

/* specify the query to select data to be scored. This query will be used as input to following R script */
declare @GetData2Score nvarchar(max) 
set @GetData2Score =  'select * from ' + @inputtable + '_Acct_Processed_Features where label<=1';

/* Get the database name*/
DECLARE @database_name varchar(max) = db_name();

/* R script to do scoring and save scored dataset into sql table */
exec sp_execute_external_script @language = N'R',
                                  @script = N'
## Get the trained model
# Define connectioin string
connection_string <- paste("Driver=SQL Server;Server=localhost;Database=", database_name, ";Trusted_Connection=true;", sep="")
# Create an Odbc connection with SQL Server using the name of the table storing the model
OdbcModel <- RxOdbcData(table = "Trained_Model", connectionString = connection_string) 
# Read the model from SQL.  
boosted_fit <- rxReadObject(OdbcModel, "Gradient Boosted Tree")

## Point to testing data in sql server
test_sql <- RxSqlServerData(sqlQuery = sprintf("%s", inquery),
							connectionString = connection_string,
							stringsAsFactors = TRUE)

## Specify the pointer to output table
Predictions_gbt_sql <- RxSqlServerData(table = outputtable, connectionString = connection_string)

## Set the Compute Context to SQL.
sql <- RxInSqlServer(connectionString = connection_string)
#rxSetComputeContext(sql) 

## Scoring
library("MicrosoftML")
rxPredict(modelObject = boosted_fit,
          data = test_sql,
		  outData = Predictions_gbt_sql,
		  overwrite = T,
		  extraVarsToWrite = c("accountID", "transactionID", "transactionDateTime", "transactionAmountUSD", "label"))

'
 , @params = N' @inquery nvarchar(max), @database_name varchar(max), @outputtable nvarchar(max)'
 , @inquery = @GetData2Score
 , @database_name = @database_name
 , @outputtable = @outputtable
 ;
end
GO
/****** Object:  StoredProcedure [dbo].[Preprocess]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[Preprocess] @table nvarchar(max)
as
begin

/* drop view if exists */
declare 
@sql_dropview nvarchar(max) = '';
set @sql_dropview = '
DROP VIEW IF EXISTS ' + @table + '_Processed'
exec sp_executesql @sql_dropview;

/* create a veiw to do preprocessing */
declare @sql_process nvarchar(max) = '';
set @sql_process = '
create view ' + @table + '_Processed as
select
label,
accountID,
transactionID,
transactionDateTime,
isnull(isProxyIP, ''0'') as isProxyIP, 
isnull(paymentInstrumentType, ''0'') as paymentInstrumentType,
isnull(cardType, ''0'') as cardType,
isnull(paymentBillingAddress, ''0'') as paymentBillingAddress,
isnull(paymentBillingPostalCode, ''0'') as paymentBillingPostalCode,
isnull(paymentBillingCountryCode, ''0'') as paymentBillingCountryCode,
isnull(paymentBillingName, ''0'') as paymentBillingName,
isnull(accountAddress, ''0'') as accountAddress,
isnull(accountPostalCode, ''0'') as accountPostalCode,
isnull(accountCountry, ''0'') as accountCountry,
isnull(accountOwnerName, ''0'') as accountOwnerName,
isnull(shippingAddress, ''0'') as shippingAddress,
isnull(transactionCurrencyCode, ''0'') as transactionCurrencyCode,
isnull(localHour,''-99'') as localHour,
isnull(ipState, ''0'') as ipState,
isnull(ipPostCode, ''0'') as ipPostCode,
isnull(ipCountryCode, ''0'') as ipCountryCode,
isnull(browserLanguage, ''0'') as browserLanguage,
isnull(paymentBillingState, ''0'') as paymentBillingState,
isnull(accountState, ''0'') as accountState,
case when isnumeric(transactionAmountUSD)=1 then cast(transactionAmountUSD as float) else 0 end as transactionAmountUSD,
case when isnumeric(digitalItemCount)=1 then cast(digitalItemCount as float) else 0 end as digitalItemCount,
case when isnumeric(physicalItemCount)=1 then cast(physicalItemCount as float) else 0 end as physicalItemCount,
case when isnumeric(accountAge)=1 then cast(accountAge as float) else 0 end as accountAge,
case when isnumeric(paymentInstrumentAgeInAccount)=1 then cast(paymentInstrumentAgeInAccount as float) else 0 end as paymentInstrumentAgeInAccount,
case when isnumeric(numPaymentRejects1dPerUser)=1 then cast(numPaymentRejects1dPerUser as float) else 0 end as numPaymentRejects1dPerUser,
isUserRegistered = case when isUserRegistered like ''%[0-9]%'' then ''0'' else isUserRegistered end
from ' + @table + '
where cast(transactionAmountUSD as float) >= 0 and   
      (case when transactionDateTime is null then 1 else 0 end) = 0 and
	  label < 2' 

exec sp_executesql @sql_process
end


GO
/****** Object:  StoredProcedure [dbo].[Save2TransactionHistory]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[Save2TransactionHistory] @table nvarchar(max), 
                                         @truncateflag nvarchar(max) 
as
begin

/* truncate historical table if truncateflag = '1' */
declare @truncatetable nvarchar(max) = '';
set @truncatetable = 'if cast(' + @truncateflag + ' as int) = 1 truncate table Transaction_History'
exec sp_executesql @truncatetable

/* insert transactions into historical table */
declare @sql_save2history nvarchar(max) = '';
set @sql_save2history ='
insert into Transaction_History
select accountID, transactionID, transactionDateTime, transactionAmountUSD from ' + @table + ';'
exec sp_executesql @sql_save2history

end
GO
/****** Object:  StoredProcedure [dbo].[ScoreOneTrans]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[ScoreOneTrans] @inputstring VARCHAR(MAX)
as
begin

/* invoke ParseStr */
declare @invokeParseStr nvarchar(max)
set @invokeParseStr ='
exec ParseStr ''' + @inputstring + ''''
exec sp_executesql @invokeParseStr

/* invoke PredictR */
declare @invokePredictR nvarchar(max)
set @invokePredictR ='
exec PredictR ''Parsed_String'', ''Predict_Score_Single_Transaction'',''1''
'
exec sp_executesql @invokePredictR
SELECT  [Probability.1]  FROM [Fraud].[dbo].[Predict_Score_Single_Transaction]

end 
GO
/****** Object:  StoredProcedure [dbo].[sortAcctTable]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[sortAcctTable] @table nvarchar(max)
as
begin

declare @dropTable nvarchar(max) 
set @dropTable = '
drop table if exists ' + @table + '_Sort'
exec sp_executesql @dropTable

declare @sortAcctTableQuery nvarchar(max) 
set @sortAcctTableQuery = '
select *,
convert(datetime,stuff(stuff(stuff(concat(transactionDate,dbo.FormatTime(transactionTime)), 9, 0, '' ''), 12, 0, '':''), 15, 0, '':'')) as recordDateTime
into ' + @table + '_Sort from ' + @table + '
order by accountID, recordDateTime desc
'
exec sp_executesql @sortAcctTableQuery
end
GO
/****** Object:  StoredProcedure [dbo].[SplitData]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[SplitData] @table varchar(max)
as
begin


/* hash accountID into 100 bins and split */

declare @hashacctNsplit nvarchar(max)
set @hashacctNsplit ='
DROP TABLE IF EXISTS Tagged_Training
DROP TABLE IF EXISTS Tagged_Testing
DROP TABLE IF EXISTS Hash_Id

select accountID,
abs(CAST(CAST(HashBytes(''MD5'', accountID) AS VARBINARY(64)) AS BIGINT) % 100) as hashCode 
into Hash_Id
from ' + @table + '

select * into Tagged_Training
from ' +@table + '
where accountID in (select accountID from Hash_Id where hashCode <= 70)

select * into Tagged_Testing
from ' +@table + '
where accountID in (select accountID from Hash_Id where hashCode > 70)'

exec sp_executesql @hashacctNsplit

end
GO
/****** Object:  StoredProcedure [dbo].[Tagging]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[Tagging]
@untaggedtable varchar(max),
@fraudtable varchar(max)
as
begin

DROP TABLE IF EXISTS Tagged;

/***********************************************************************/
/* reformat transactionTime and create transactionDateTime for fraud transactions*/
/**********************************************************************/
/* ##table is a global temporary table which will be written only once to temporary database */ 
declare @maketransactionDateTime nvarchar(max)
set @maketransactionDateTime = 
'
select *,
  convert(datetime,stuff(stuff(stuff(concat(transactionDate,dbo.FormatTime(transactionTime)), 9, 0, '' ''), 12, 0, '':''), 15, 0, '':'')) as transactionDateTime
into ##Formatted_Fraud
from ' + @fraudtable

exec sp_executesql @maketransactionDateTime
/*****************************************************************************************************************/
/* remove duplicate based on keys: transactionID, accountID, transactionDate, transactionDate, transactionAmount */
/*****************************************************************************************************************/
/* sometimes an entire transaction might be divided into multiple sub-transactions. thus, even transactionID, accountID, transactionDate/Time are same, the amount might be different */
declare @removeduplicates1 nvarchar(max)
set @removeduplicates1 = 
';WITH cte_1
     AS (SELECT ROW_NUMBER() OVER (PARTITION BY transactionID, accountID, transactionDateTime, transactionAmount
                                       ORDER BY transactionID ASC) RN 
         FROM ' + @untaggedtable + ')
DELETE FROM cte_1
WHERE  RN > 1;'
exec sp_executesql @removeduplicates1

;WITH cte_2
     AS (SELECT ROW_NUMBER() OVER (PARTITION BY transactionID, accountID, transactionDate, transactionDateTime, transactionAmount
                                       ORDER BY transactionID ASC) RN 
         FROM ##Formatted_Fraud)
DELETE FROM cte_2
WHERE  RN > 1;


/*********************************************************************************************************************/
/* tagging on account level:  
   if accountID can't be found in fraud dataset => tag as 0, non fraud
   if accountID found in fraud dataset but transactionDateTime is out of the fraud time range => tag as 2, pre-fraud
   if accountID found in fraud dataset and transactionDateTime is within the fraud time range => tag as 1, fraud */
/**********************************************************************************************************************/
/* convert fraud to account level and create start and end date time */
select accountID, min(transactionDateTime) as startDateNTime,  max(transactionDateTime) as endDateNTime
into ##Fraud_Account
from ##Formatted_Fraud 
group by accountID


/* Tagging */ 
declare @tagging nvarchar(max)
set @tagging = 
'select t.*, 
	   case 
         when (sDT is not null and tDT >= sDT and tDT <= eDT) then 1
		 when (sDT is not null and tDT < sDT) then 2
		 when (sDT is not null and tDT > eDT) then 2
		 when sDT is null then 0
	   end as label
into Tagged
from 
(select t1.*,
  t1.transactionDateTime as tDT,
  t2.startDateNTime as sDT,
  t2.endDateNTime as eDT
 from ' + @untaggedtable + ' as t1
 left join
 ##Fraud_Account as t2
 on t1.accountID = t2.accountID
 ) t'
exec sp_executesql @tagging

drop table ##Fraud_Account
drop table ##Formatted_Fraud 
end
GO
/****** Object:  StoredProcedure [dbo].[TrainModelR]    Script Date: 10/4/2017 9:51:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[TrainModelR] @table nvarchar(max)
as
begin

/* Create an empty table to be filled with the trained models */
if exists 
(select * from sysobjects where name like 'Trained_Model') 
truncate table Trained_Model
else
create table Trained_Model ( 
 id varchar(200) not null,
 value varbinary(max)
 --,constraint unique_id3 unique(id)
);

/* down sample the majority by: 
1. sort the data by label and accountID in descent order 
2. select the top 10000 rows
*/
declare @GetTrainData nvarchar(max) 
set @GetTrainData =  'select * from ' + @table

/*Get the database name*/
DECLARE @database_name nvarchar(max) = db_name();

/* R script to train GBT model and save the trained model into a sql table */
execute sp_execute_external_script
  @language = N'R',
  @script = N' 
  # define the connection string
  connection_string <- paste("Driver=SQL Server;Server=localhost;Database=", database_name, ";Trusted_Connection=true;", sep="")

  # Set the Compute Context to SQL for faster training.
  sql <- RxInSqlServer(connectionString = connection_string)
  rxSetComputeContext(sql)

  ## Point to testing data in sql server
  train_sql <- RxSqlServerData(sqlQuery = sprintf("%s", inquery),
							   connectionString = connection_string,
							   stringsAsFactors = TRUE)

  ## make equations
  variables_all <- rxGetVarNames(train_sql)
  variables_to_remove <- c("label", "accountID", "transactionID", "transactionDateTime")
  training_variables <- variables_all[!(variables_all %in% variables_to_remove)]
  equation <- paste("label ~ ", paste(training_variables, collapse = "+", sep=""), sep="")

  ## train GBT model
  library("MicrosoftML")
  boosted_fit <- rxFastTrees(formula = as.formula(equation),
                             data = train_sql,
                             type = c("binary"),
                             numTrees = 100,
                             learningRate = 0.2,
                             splitFraction = 5/24,
                             featureFraction = 1,
                             minSplit = 10,
							 unbalancedSets = TRUE,
							 randomSeed = 5)

  ## save the trained model in sql server 
  # set the compute context to local for tables exportation to SQL
  rxSetComputeContext("local")
  # Open an Odbc connection with SQL Server. 
  OdbcModel <- RxOdbcData(table = "Trained_Model", connectionString = connection_string) 
  rxOpen(OdbcModel, "w") 
  # Write the model to SQL.  
  rxWriteObject(OdbcModel, "Gradient Boosted Tree", boosted_fit)
 
  '
  , @params = N' @inquery nvarchar(max), @database_name varchar(max)'
  , @inquery = @GetTrainData
  , @database_name = @database_name
 ;
end
GO
USE [master]
GO
ALTER DATABASE [Fraud] SET  READ_WRITE 
GO