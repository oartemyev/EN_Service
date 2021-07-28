package main

import (
	"bufio"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"

	//"path/filepath"

	//	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/text/encoding/charmap"

	"database/sql"

	"context"

	mssql "github.com/denisenkom/go-mssqldb"
	//	iconv "github.com/djimenez/iconv-go"
)

var db *sql.DB
var szFirma, szFileName string

func ScanDir() []fs.FileInfo {
	tmp := Cfg.GetValue("ScanDir", "")
	if tmp == "" {
		log.Printf("ScanDir=%s", tmp)
		return make([]fs.FileInfo, 0)
	}
	files, err := ioutil.ReadDir(tmp)
	if err != nil {
		log.Fatal(err)
		return make([]fs.FileInfo, 0)
	}

	return files
}

func DecodeWindows1251(ba string) string {
	dec := charmap.Windows1251.NewDecoder()
	out, _ := dec.Bytes([]byte(ba))
	return string(out)
}

func GetFirma(db *sql.DB, sz string) string {

	rows, err := db.Query(fmt.Sprintf("SELECT NAME FROM Firma WHERE FirmaID=%s", sz))
	if err != nil {
		log.Println(err)
		szFirma = ""
		return szFirma
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&szFirma)
		if err != nil {
			log.Println(err)
			szFirma = ""
			return szFirma
		}
	}

	return szFirma
}

func DateToSqlFull(szDate string, szTime string) string {
	if strings.Trim(szDate, " \t") == ".  ." {
		return "1754-01-01T00:00:00.000"
	}

	var tm time.Time
	var err error
	var yy, mm, dd int

	a := strings.Split(szDate, ".")
	yy, err = strconv.Atoi(a[2])
	if err != nil {
		return "1754-01-01T00:00:00.000"
	}
	mm, err = strconv.Atoi(a[1])
	if err != nil {
		return "1754-01-01T00:00:00.000"
	}
	dd, err = strconv.Atoi(a[0])
	if err != nil {
		return "1754-01-01T00:00:00.000"
	}

	if yy < 1900 {
		yy = yy + 2000
	}

	// dt, err = time.Parse("02.01.2006", szDate)
	// if err != nil {
	// 	return "1754-01-01T00:00:00.000"
	// }
	tm, err = time.Parse("15:04:05", szTime)
	if err != nil {
		return fmt.Sprintf("%04d-%02d-%02dT00:00:00.000", yy, mm, dd)
	}
	return fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d.000", yy, mm, dd, tm.Hour(), tm.Minute(), tm.Second())
}

func DateToSql(szDate string) string {
	if strings.Trim(szDate, " \t") == ".  ." {
		return "1754-01-01T00:00:00.000"
	}

	var err error
	var yy, mm, dd int

	a := strings.Split(szDate, ".")
	yy, err = strconv.Atoi(a[2])
	if err != nil {
		return "1754-01-01T00:00:00.000"
	}
	mm, err = strconv.Atoi(a[1])
	if err != nil {
		return "1754-01-01T00:00:00.000"
	}
	dd, err = strconv.Atoi(a[0])
	if err != nil {
		return "1754-01-01T00:00:00.000"
	}

	if yy < 1900 {
		yy = yy + 2000
	}

	return fmt.Sprintf("%04d-%02d-%02dT00:00:00.000", yy, mm, dd)
}

func GetNumberDoc(sz string) string {
	a := strings.Split(sz, "|")
	return a[2]
}

func GetPrefixInsertPrihod() string {
	return "INSERT INTO #KKMSDK (FirmaID,SkladID,ClientID,ArticleID,Document,Data,PriceStart,PriceEnd,Quantity,BaseSum,PriceOut,NumDoc) VALUES"
}

func GetTimeRange(sT1 string, sT2 string) int {
	var dt1, dt2 time.Time
	var err error
	dt1, err = time.Parse("15:04", sT1)
	if err != nil {
		return 0
	}
	dt2, err = time.Parse("15:04", sT1)
	if err != nil {
		return 0
	}

	return dt2.Hour()*60 + dt2.Minute() - (dt1.Hour()*60 + dt1.Minute())
}

//
//  0 - Все хорошо
//  2 - ошибка при обращении к БД
//
func SaleInClient(db *sql.DB, sc *bufio.Scanner) int {
	var iRet int = 0
	//var bTime int = 0
	var Month, Year, FirmaID int
	var a []string

	ctx := context.Background()
	_err := db.PingContext(ctx)
	if _err != nil {
		log.Println(_err)
		return 2
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		log.Println(_err)
		return 2
	}
	defer conn.Close()

	bFlag := 0

	conn.ExecContext(ctx, "DROP TABLE #KKMSDK")
	_, err = conn.ExecContext(ctx, `
	CREATE TABLE #KKMSDK(
		[ID] [int] IDENTITY(1,1) NOT NULL,
		[FirmaID] [int] NULL,
		[Year] [int] NULL,
		[Month] [int] NULL,
		[EZ] [int] NULL,
		[ClientID] [int] NULL,
		[SOstatok] [money] NULL,
		[Zakup] [money] NULL,
		[Seb] [money] NULL,
		[Sale] [money] NULL,
		[Ret] [money] NULL,
		[Spis] [money] NULL,
		[Oborot] [money] NULL,
		[Ostatok] [money] NULL,
		[Zapas] [money] NULL,
	)
		`)
	if err != nil {
		log.Println(err)
		return 2
	}

	nDataInsert := 0
	Query := ""
	for sc.Scan() {
		//str, _ := iconv.ConvertString(strings.Trim(sc.Text(), " \t"), "windows-1252", "utf-8")
		str := DecodeWindows1251(strings.Trim(sc.Text(), " \t"))
		if str == "=КОНЕЦ ПАКЕТА=" {
			break
		}
		a = strings.Split(str, "\t")

		if strings.ToUpper(a[0]) == "УДАЛИТЬ" {
			Month, err = strconv.Atoi(a[3])
			if err != nil {
				log.Println(err)
				return 2
			}
			Year, err = strconv.Atoi(a[2])
			if err != nil {
				log.Println(err)
				return 2
			}
			FirmaID, err = strconv.Atoi(a[1])
			if err != nil {
				log.Println(err)
				return 2
			}

			log.Printf("Загрузка 'Клиенты + Закупки' от %02d.%04d из %s", Month, Year, GetFirma(db, a[1]))
			Query = fmt.Sprintf("DELETE FROM SaleInClient WHERE FirmaID=%d AND [Year]=%d AND [Month]=%d", FirmaID, Year, Month)
			_, err = conn.ExecContext(ctx, Query)
			if err != nil {
				log.Println(err)
				return 2
			}

			continue
		}

		if nDataInsert >= 100 {
			_, err = conn.ExecContext(ctx, Query)
			if err != nil {
				log.Println(err)
				return 2
			}
			nDataInsert = 0
			Query = ""
		}
		//
		// Поля:
		// 0 - FirmaID
		// 1 - ArticleID
		// 2 - Data
		// 3 - Quantity
		// 4 - BaseSum
		// 5 - Document
		//

		Query = Query + "\nINSERT INTO #KKMSDK (FirmaID,[Year],[Month],EZ,ClientID,SOstatok,Zakup,Seb,Sale,Ret,Spis,Oborot,Ostatok,Zapas) VALUES("
		for i := 0; i < len(a); i++ {
			if i == 0 {
				Query = Query + a[i]
			} else {
				Query = Query + fmt.Sprintf(",%s", a[i])
			}
		}
		Query = Query + ")"
		nDataInsert++
		if bFlag == 0 {
			bFlag = 1
			GetFirma(db, a[0])
			log.Printf("ОтгрузкаРЦ от %s № %s из магазина %s", a[3], a[9], szFirma)
		}
	}

	if nDataInsert >= 0 {
		_, err = conn.ExecContext(ctx, Query)
		if err != nil {
			log.Println(err)
			return 2
		}
		nDataInsert = 0
		Query = ""
	}
	_, err = conn.ExecContext(ctx, `DECLARE @ReturnCode 	int, @SQLError	int, @ErrorMask int
	exec SaleInClientUpdate @ReturnCode, @SQLError, @ErrorMask
	`)
	if err != nil {
		log.Printf("ОШИБКА %s в процедуре 'SaleInClientUpdate'", err.Error())
		return 2
	}
	log.Printf("Загрузили 'Клиенты + Закупки'  от %02d.%04d из %s", Month, Year, GetFirma(db, a[0]))

	return iRet
}

//
//  0 - Все хорошо
//  2 - ошибка при обращении к БД
//
func OtgruzkaRC(db *sql.DB, sc *bufio.Scanner) int {
	var iRet int = 0
	//var bTime int = 0
	var szTime string

	ctx := context.Background()
	_err := db.PingContext(ctx)
	if _err != nil {
		log.Println(_err)
		return 2
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		log.Println(_err)
		return 2
	}
	defer conn.Close()

	bFlag := 0

	conn.ExecContext(ctx, "DROP TABLE #KKMSDK")
	_, err = conn.ExecContext(ctx, `
	CREATE TABLE #KKMSDK(
		[FirmaRcID] [int] NULL,
		[FirmaID] [int] NULL,
		[Data] [datetime] NULL,
		[DocNo] [varchar](30) NULL,
		[ArticleID] [int] NULL,
		[Quantity] [money] NULL,
		[QuantityReal] [money] NULL,
		[Closed] [int] NULL,
		[Document] [varchar](40) NULL,
		[SummaSeb] [money] NULL,
		[PriceSeb] [money] NULL,
		[PriceSpec] [money] NULL,
		[PriceIn] [money] NULL,
		[Status] [int] NULL,
		[SumNDS] [money] NULL
	)
	`)
	if err != nil {
		log.Println(err)
		return 2
	}

	nDataInsert := 0
	Query := ""
	for sc.Scan() {
		//str, _ := iconv.ConvertString(strings.Trim(sc.Text(), " \t"), "windows-1252", "utf-8")
		str := DecodeWindows1251(strings.Trim(sc.Text(), " \t"))
		if str == "=КОНЕЦ ПАКЕТА=" {
			break
		}
		a := strings.Split(str, "\t")
		if strings.ToUpper(a[0]) == "ВРЕМЯ_ДОКУМЕНТА" {
			szTime = a[1]
			//bTime = 1
			log.Printf("ВРЕМЯ_ДОКУМЕНТА=%s", szTime)
			continue
		}

		if nDataInsert >= 100 {
			_, err = conn.ExecContext(ctx, Query)
			if err != nil {
				log.Println(err)
				return 2
			}
			nDataInsert = 0
			Query = ""
		}
		//
		// Поля:
		//		1 - Код фирмы РЦ
		//		2 - Код магазина
		//		3 - Дата документа
		//		4 - Время документа
		//		5 - Код товара
		//		6 - Количество отгруженное
		//		7 - количество принятое
		//		8 - ДокИДД
		//		9 - Флаг того что магазин оприходовал
		//	   10 - Номер документа
		//
		if len(a) < 9 {
			log.Printf("ФАЙЛ - %s", szFileName)
			log.Printf("ОШИБКА: кол-во полей в Отгрузке из РЦ=%d", len(a))
			break
		}

		szData := DateToSqlFull(a[2], a[3])

		Query = Query + "\nINSERT INTO #KKMSDK (FirmaRcID,FirmaID,ArticleID,Data,Quantity,QuantityReal,Document,DocNo,Closed) VALUES("
		Query = Query + fmt.Sprintf("%s,%s,%s,'%s',%s,%s,'%s','%s',%s)", a[0], a[1], a[4], szData, a[5], a[6], a[7], a[9], a[8])
		nDataInsert++
		if bFlag == 0 {
			bFlag = 1
			GetFirma(db, a[0])
			log.Printf("ОтгрузкаРЦ от %s № %s из магазина %s", a[3], a[9], szFirma)
		}
	}

	if nDataInsert >= 0 {
		_, err = conn.ExecContext(ctx, Query)
		if err != nil {
			log.Println(err)
			return 2
		}
		nDataInsert = 0
		Query = ""
	}
	_, err = conn.ExecContext(ctx, `DECLARE @ReturnCode 	int, @SQLError	int, @ErrorMask int
	exec OtgruzkaRCUpdate @ReturnCode, @SQLError, @ErrorMask
	`)
	if err != nil {
		log.Printf("ОШИБКА %s в процедуре 'OtgruzkaRCUpdate'", err.Error())
		return 2
	}

	return iRet
}

//
//  0 - Все хорошо
//  2 - ошибка при обращении к БД
//
func CloseZakaz(db *sql.DB, sc *bufio.Scanner) int {
	var iRet int = 0

	ctx := context.Background()
	_err := db.PingContext(ctx)
	if _err != nil {
		log.Println(_err)
		return 2
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		log.Println(_err)
		return 2
	}
	defer conn.Close()

	//bFlag := 0

	conn.ExecContext(ctx, "DROP TABLE #KKMSDK")
	_, err = conn.ExecContext(ctx, `
	CREATE TABLE #KKMSDK(
		[FirmaID] [int] NOT NULL,
		[SkladID] [int] NOT NULL,
		[ClientID] [int] NOT NULL,
		[Data] [datetime] NOT NULL,
		[ZakazNum] [char](10) NOT NULL,
		[Document] [char](30) NOT NULL
		)
	`)
	if err != nil {
		log.Println(err)
		return 2
	}

	var FirmaID, SkladID, ClientID int
	var szData, szDoc, szListZakaz string

	//nDataInsert := 0
	Query := ""
	for sc.Scan() {
		//str, _ := iconv.ConvertString(strings.Trim(sc.Text(), " \t"), "windows-1252", "utf-8")
		str := DecodeWindows1251(strings.Trim(sc.Text(), " \t"))
		if str == "=КОНЕЦ ПАКЕТА=" {
			break
		}
		a := strings.Split(str, "\t")
		if strings.ToUpper(strings.Trim(a[0], " \t")) == "ФИРМА" {
			FirmaID, _ = strconv.Atoi(a[3])
			GetFirma(db, a[3])
			continue
		}
		if strings.ToUpper(strings.Trim(a[0], " \t")) == "СКЛАД" {
			SkladID = 0
			continue
		}
		if strings.ToUpper(strings.Trim(a[0], " \t")) == "КЛИЕНТ" {
			ClientID, _ = strconv.Atoi(a[3])
			continue
		}
		if strings.ToUpper(strings.Trim(a[0], " \t")) == "ДАТАДОК" {
			szData = DateToSql(a[2])
			continue
		}
		if strings.ToUpper(strings.Trim(a[0], " \t")) == "ДОКМАГ" {
			szDoc = strings.Trim(a[2], " ")
			continue
		}
		if strings.ToUpper(strings.Trim(a[0], " \t")) == "СПИСОКЗАКАЗОВ" {
			if len(a) < 3 {
				return 0
			}
			szListZakaz = strings.Trim(a[2], " ")
		}
		_, err = conn.ExecContext(ctx, Query)
		if err != nil {
			log.Println(err)
			return 2
		}
		Query = ""
	}
	a := strings.Split(szListZakaz, "\t")
	for i := 0; i < len(a); i++ {
		Query = Query + "INSERT INTO #KKMSDK (FirmaID,SkladID,ClientID,Data,ZakazNum,Document) VALUES("
		Query = Query + fmt.Sprintf("%d,%d,%d,'%s','%s','%s')", FirmaID, SkladID, ClientID, szData, a[i], szDoc)

	}
	_, err = conn.ExecContext(ctx, `DECLARE @ReturnCode 	int, @SQLError	int, @ErrorMask int
	exec ZakazPrihodUpdate @ReturnCode, @SQLError, @ErrorMask
	`)
	if err != nil {
		log.Printf("ОШИБКА %s в процедуре 'ZakazPrihodUpdate'", err.Error())
		return 2
	}

	log.Printf("Закрытие заказов по приходу от %s № %s по магазину %s", szData, GetNumberDoc(szDoc), szFirma)

	return iRet
}
func LoadPereocenka(db *sql.DB, sc *bufio.Scanner) int {
	var iRet int = 0
	var szTime string

	ctx := context.Background()
	_err := db.PingContext(ctx)
	if _err != nil {
		log.Println(_err)
		return 2
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		log.Println(_err)
		return 2
	}
	defer conn.Close()

	conn.ExecContext(ctx, "DROP TABLE #KKMSDK")

	//sqlr, err := db.ExecContext(ctx, "SELECT * INTO #KKMSDK FROM ChangePrice WHERE 1=0")
	_, err = conn.ExecContext(ctx, `
	 	CREATE TABLE #KKMSDK
	 (
			FirmaID   int,			
			ArticleID int,			
			PriceIn   money,		
			PriceOut  money,		
			Data      DateTime,		
			Document  varchar(80) COLLATE Cyrillic_General_CI_AS,	
			DocName   varchar(30) COLLATE Cyrillic_General_CI_AS,	
			NumDoc    varchar(20) COLLATE Cyrillic_General_CI_AS,	
			DocFixIDD varchar(80) COLLATE Cyrillic_General_CI_AS	
		)
		`)
	if err != nil {
		log.Println(err)
		return 2
	}

	for sc.Scan() {
		//str, _ := iconv.ConvertString(strings.Trim(sc.Text(), " \t"), "windows-1252", "utf-8")
		str := DecodeWindows1251(strings.Trim(sc.Text(), " \t"))
		if str == "=КОНЕЦ ПАКЕТА=" {
			break
		}
		a := strings.Split(str, "\t")
		if strings.ToUpper(a[0]) == "ВРЕМЯ_ДОКУМЕНТА" {
			szTime = a[1]
			//bTime = 1
			log.Printf("ВРЕМЯ_ДОКУМЕНТА=%s", szTime)
			continue
		}
		if (strings.ToUpper(a[0]) == "КОМАНДА") && (strings.ToUpper(a[1]) == "УДАЛИТЬ") {
			_, err = conn.ExecContext(ctx, `DECLARE @ReturnCode 	int, @SQLError	int, @ErrorMask int
			exec i_PereocenkaDelete @ReturnCode, @SQLError, '`+a[2]+`,@ErrorMask
			`)
			if err != nil {
				log.Printf("ОШИБКА %s в процедуре 'i_PereocenkaDelete'", err.Error())
				return 2
			}
			continue
		}
	}
	return iRet
}

//
//  0 - Все хорошо
//  2 - ошибка при обращении к БД
//
func Prihod(db *sql.DB, sc *bufio.Scanner) int {
	var iRet int = 0
	var bTime int = 0
	var szTime, szNameDoc string

	ctx := context.Background()
	_err := db.PingContext(ctx)
	if _err != nil {
		log.Println(_err)
		return 2
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		log.Println(_err)
		return 2
	}
	defer conn.Close()

	bDel := 0
	bFlag := 0
	szSklad := "0"
	szFirma = ""

	conn.ExecContext(ctx, "DROP TABLE #KKMSDK")

	//sqlr, err := db.ExecContext(ctx, "SELECT * INTO #KKMSDK FROM ChangePrice WHERE 1=0")
	_, err = conn.ExecContext(ctx, `
	 	CREATE TABLE #KKMSDK
	 (
	 	[FirmaID] [int] NOT NULL,
		[SkladID] [int] NOT NULL,
		[ClientID] [int] NOT NULL,
		[ArticleID] [int] NOT NULL,
		[Document] [char](80) NOT NULL,
		[Data] [datetime] NOT NULL,
		[PriceStart] [money] NULL,
		[PriceEnd] [money] NULL,
		[Quantity] [money] NULL,
		[BaseSum] [money] NULL,
		[PriceOut] [money] NULL,
		[TimeS] [varchar](8) NULL,
		[TimeE] [varchar](8) NULL,
		[SizeTime] [int] NULL,
		[PosCount] [int] NULL,
		[DateLoad] [datetime] NULL,
		[NumDoc] [varchar](20) NULL,
		[DocName] [varchar](30) NULL,
		[DESADV_GUID] [char](36) NULL,
		[CloseSF] [datetime] NULL,
		[NumTTN] [varchar](20) NULL,
		[DataTTN] [datetime] NULL
	)
		`)
	if err != nil {
		log.Println(err)
		return 2
	}
	//log.Print(sqlr)

	nDataInsert := 0
	Query := ""
	for sc.Scan() {
		//str, _ := iconv.ConvertString(strings.Trim(sc.Text(), " \t"), "windows-1252", "utf-8")
		str := DecodeWindows1251(strings.Trim(sc.Text(), " \t"))
		if str == "=КОНЕЦ ПАКЕТА=" {
			break
		}
		a := strings.Split(str, "\t")
		if strings.ToUpper(a[0]) == "ВРЕМЯ_ДОКУМЕНТА" {
			szTime = a[1]
			bTime = 1
			log.Printf("ВРЕМЯ_ДОКУМЕНТА=%s", szTime)
			continue
		}
		if strings.ToUpper(a[0]) == "УДАЛИТЬ" {
			GetFirma(db, a[1])
			szNumDoc := a[2]
			log.Printf("(DEL) Приходная накладная от № %s  из магазина %s", szNumDoc, szFirma)
			_, err = db.ExecContext(ctx, fmt.Sprintf("DELETE PrihodItem FROM Prihod WHERE Prihod.Document = '%s' AND PrihodItem.PrihodID = Prihod.PrihodID", szNumDoc))
			if err != nil {
				log.Println(err)
				return 2
			}
			_, err = db.ExecContext(ctx, fmt.Sprintf("DELETE Prihod WHERE Document = '%s'", szNumDoc))
			if err != nil {
				log.Println(err)
				return 2
			}
			_, err = db.ExecContext(ctx, fmt.Sprintf("DELETE DeclarantPrihod WHERE Document = '%s'", szNumDoc))
			if err != nil {
				log.Println(err)
				return 2
			}
			bDel = 1
			continue
		}
		if len(a) < 11 {
			log.Printf("ФАЙЛ - %s", szFileName)
			log.Printf("ОШИБКА: кол-во полей в приходной накладной=%d", len(a))
			break
		}

		if nDataInsert >= 100 {
			_, err = conn.ExecContext(ctx, Query)
			if err != nil {
				log.Println(err)
				return 2
			}
			nDataInsert = 0
			Query = ""
		}
		szNumDoc := ""
		szData := ""
		if len(a) == 11 {
			if bTime == 1 {
				szData = DateToSqlFull(a[5], szTime)
			} else {
				szData = DateToSql(a[5])
			}
			szNumDoc = GetNumberDoc(a[4])
			Query = Query + "\nINSERT INTO #KKMSDK (FirmaID,SkladID,ClientID,ArticleID,Document,Data,PriceStart,PriceEnd,Quantity,BaseSum,PriceOut,NumDoc) VALUES"
			Query = Query + fmt.Sprintf("(%s,%s,%s,%s,'%s','%s',%s,%s,%s,%s,%s,'%s')", a[0], szSklad, a[2], a[3], a[4], szData, a[6], a[7], a[8], a[9], a[10], szNumDoc)
			nDataInsert++
		} else {
			if bTime == 1 {
				szData = DateToSqlFull(a[5], szTime)
			} else {
				szData = DateToSql(a[5])
			}
			szNameDoc = ""
			szDataDoc := szData
			if len(a) == 16 {
				szNumDoc = strings.Trim(a[15], " ")
				szNameDoc = "ПриходнаяНакладная"
			} else if (len(a) == 18) || (len(a) == 19) {
				szNumDoc = a[15]
				szNameDoc = a[17]
			} else {
				szNumDoc = GetNumberDoc(a[4])
				szNameDoc = "ПриходнаяНакладная"
			}
			if len(a) == 19 {
				SizeTime := GetTimeRange(a[12], a[13])
				if nDataInsert > 0 {
					Query = Query + ","
				}
				Query = Query + "\nINSERT INTO #KKMSDK (FirmaID,SkladID,ClientID,ArticleID,Document,Data,PriceStart,PriceEnd,Quantity,BaseSum,PriceOut,TimeS,TimeE,SizeTime,PosCount,DateLoad,NumDoc,DocName,DESADV_GUID) VALUES" +
					fmt.Sprintf("(%s,%s,%s,%s,'%s','%s',%s,%s,%s,%s,%s,'%s','%s',%d,%s,'%s','%s','%s','%s')",
						a[0], szSklad, a[2], a[3], a[4], szDataDoc, a[6], a[7], a[8], a[9], a[10],
						a[12], a[13], SizeTime, a[14], DateToSql(a[11]), szNumDoc, szNameDoc, a[18])
				nDataInsert++
			} else if len(a) == 20 {
				SizeTime := GetTimeRange(a[12], a[13])
				Query = Query + "\nINSERT INTO #KKMSDK (FirmaID,SkladID,ClientID,ArticleID,Document,Data,PriceStart,PriceEnd,Quantity,BaseSum,PriceOut,TimeS,TimeE,SizeTime,PosCount,DateLoad,NumDoc,DocName,DESADV_GUID,CloseSF) VALUES" +
					fmt.Sprintf("(%s,%s,%s,%s,'%s','%s',%s,%s,%s,%s,%s,'%s','%s',%d,%s,'%s','%s','%s','%s','%s')",
						a[0], szSklad, a[2], a[3], a[4], szDataDoc, a[6], a[7], a[8], a[9], a[10],
						a[12], a[13], SizeTime, a[14], DateToSql(a[11]), szNumDoc, szNameDoc, a[18], a[19])
				nDataInsert++
			} else if len(a) == 22 {
				szDataTTN := DateToSql(a[21])
				szDataSF := "17530101"
				SizeTime := GetTimeRange(a[12], a[13])
				if strings.Trim(a[19], " ") != "" {
					szDataSF = strings.Trim(a[19], " ")
				}
				Query = Query + "\nINSERT INTO #KKMSDK (FirmaID,SkladID,ClientID,ArticleID,Document,Data,PriceStart,PriceEnd,Quantity,BaseSum,PriceOut,TimeS,TimeE,SizeTime,PosCount,DateLoad,NumDoc,DocName,DESADV_GUID,CloseSF,NumTTN,DataTTN) VALUES" +
					fmt.Sprintf("(%s,%s,%s,%s,'%s','%s',%s,%s,%s,%s,%s,'%s','%s',%d,%s,'%s','%s','%s','%s','%s','%s','%s')",
						a[0], szSklad, a[2], a[3], a[4], szDataDoc, a[6], a[7], a[8], a[9], a[10],
						a[12], a[13], SizeTime, a[14], DateToSql(a[11]), szNumDoc, szNameDoc, a[18], szDataSF,
						a[20], szDataTTN)
				nDataInsert++
			} else {
				SizeTime := GetTimeRange(a[12], a[13])
				Query = Query + "\nINSERT INTO #KKMSDK (FirmaID,SkladID,ClientID,ArticleID,Document,Data,PriceStart,PriceEnd,Quantity,BaseSum,PriceOut,TimeS,TimeE,SizeTime,PosCount,DateLoad,NumDoc,DocName) VALUES" +
					fmt.Sprintf("(%s,%s,%s,%s,'%s','%s',%s,%s,%s,%s,%s,'%s','%s',%d,%s,'%s','%s','%s')",
						a[0], szSklad, a[2], a[3], a[4], szDataDoc, a[6],
						a[7], a[8], a[9], a[10], a[12], a[13], SizeTime, a[14], DateToSql(a[11]), szNumDoc, szNameDoc)
				nDataInsert++
			}
		}
		if bFlag == 0 {
			bFlag = 1
			GetFirma(db, a[0])
			log.Printf("%s от %s № %s из магазина %s", szNameDoc, a[5], szNumDoc, szFirma)
		}
	}

	if bDel == 0 {
		if nDataInsert > 0 {
			_, err = conn.ExecContext(ctx, Query)
			if err != nil {
				log.Println(err)

				log.Print(Query)

				return 2
			}
			nDataInsert = 0
			Query = ""
		}
		_, err = conn.ExecContext(ctx, `DECLARE @ReturnCode 	int, @SQLError	int, @ErrorMask int
		exec i_PrihodUpdate @ReturnCode, @SQLError, @ErrorMask
		`)
		if err != nil {
			log.Println(err)
			return 2
		}

	} else {
		log.Print("УДАЛИЛИ!")
	}

	return iRet
}

////////////////////////////////////////////////
//  Возврат :
//				0 - просто удалить файл
//				1 - перенести файл для обработки из 1С
//				2 - Ошибка при обращении к БД
//				3 - Перенести файл для обработки из 1С миную Lua
func ParseFile(db *sql.DB, fi fs.FileInfo) int {

	iRet := 1
	tmp := Cfg.GetValue("ScanDir", "")
	szFileName = fi.Name()
	f, err := os.Open(tmp + "\\" + fi.Name())
	if err != nil {
		panic(err)
	}
	defer f.Close()

	log.Printf("Обработка %s", szFileName)

	bFlag := 0

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		//str, _ := iconv.ConvertString(strings.Trim(sc.Text(), " \t"), "windows-1252", "utf-8")
		str := DecodeWindows1251(strings.Trim(sc.Text(), " \t"))
		if str == "=НАЧАЛО ПАКЕТА=" {
			bFlag = 1 // есть начало пакета
			continue
		}
		if str == "=КОНЕЦ ПАКЕТА=" {
			bFlag = 0 // есть конец пакета
			continue
		}
		if bFlag == 0 {
			continue
		}
		a := strings.Split(str, "\t")
		if strings.ToUpper(a[0]) == "ВИД" && strings.ToUpper(a[1]) == "РЕАЛИЗАЦИЯ_РЦ" {
			return 3
		}
		if strings.ToUpper(a[0]) == "ВИД" && strings.ToUpper(a[1]) == "MD_ФАЙЛ" {
			return 3
		}
		if strings.ToUpper(a[0]) == "ВИД" && strings.ToUpper(a[1]) == "ВИДЫ_ПРОДУКЦИИ_ПО_ТОВАРУ" {
			return 3
		}
		if strings.ToUpper(a[0]) == "ВИД" && strings.ToUpper(a[1]) == "ОБОРОТ_АЛКОГОЛЯ_ПО_ДОКУМЕНТАМ" {
			return 3
		}
		if strings.ToUpper(a[0]) == "ВИД" && strings.ToUpper(a[1]) == "ЧЕКИВЫРУЧКА_УРОВНИ" {
			return 3
		}
		if strings.ToUpper(a[0]) == "ВИД" && strings.ToUpper(a[1]) == "ЧЕКИВЫРУЧКА_ИМ" {
			return 3
		}
		if strings.ToUpper(a[0]) == "ВИД" && strings.ToUpper(a[1]) == "ПРОВЕРКАБЛОКИРОВКИ" {
			return 3
		}
		if (strings.ToUpper(a[0]) == "ВИД" || strings.ToUpper(a[0]) == "ОБЪЕКТ") && strings.ToUpper(a[1]) == "УЦЕНКА" {
			return 3
		}
		if strings.ToUpper(a[0]) == "ОБЪЕКТ" && strings.ToUpper(a[1]) == "ДОКУМЕНТ" {
			return 3
		}
		if (strings.ToUpper(a[0]) == "ВИД" || strings.ToUpper(a[0]) == "ОБЪЕКТ") && strings.ToUpper(a[1]) == "ЦЕНЫ" {
			iRet = Prihod(db, sc)
			continue
		}
		if strings.ToUpper(a[0]) == "ВИД" && strings.ToUpper(a[1]) == "ОТГРУЗКА_ИЗ_РЦ" {
			iRet = OtgruzkaRC(db, sc)
			continue
		}
		if strings.ToUpper(a[0]) == "ВИД" && strings.ToUpper(a[1]) == "ПРОДАЖИ_ЗАКУПКИ_КЛИЕНТЫ" {
			iRet = SaleInClient(db, sc)
			continue
		}
		if (strings.ToUpper(a[0]) == "ВИД" || strings.ToUpper(a[0]) == "ОБЪЕКТ") && strings.ToUpper(a[1]) == "ЗАКРЫТИЕЗАКАЗОВ" {
			iRet = CloseZakaz(db, sc)
			continue
		}
		if (strings.ToUpper(a[0]) == "ВИД" || strings.ToUpper(a[0]) == "ОБЪЕКТ") && strings.ToUpper(a[1]) == "ПЕРЕОЦЕНКИ" {
			iRet = LoadPereocenka(db, sc)
			continue
		}
	}

	return iRet
}

func MyCopyFile(szFileName string, szFileName1C string) {
	err := os.Rename(szFileName, szFileName1C)
	if err != nil {
		log.Printf("ОШИБКА при копировании %s в %s *** %s", szFileName, szFileName1C, err.Error())
	}
}

// func makeConnURL(server string, port int, user string, password string) *url.URL {
// 	return &url.URL{
// 		Scheme: "sqlserver",
// 		Host:   server + ":" + strconv.Itoa(port),
// 		User:   url.UserPassword(user, password),
// 	}
// }

func RunOnce() {
	var err error

	arFile := ScanDir()
	//fmt.Printf("%q", arFile)
	if len(arFile) == 0 {
		return
	}

	sort.SliceStable(arFile, func(i, j int) bool {
		return arFile[i].Name() < arFile[j].Name()
	})
	nSize := len(arFile)
	if nSize > 300 {
		nSize = 300
	}

	log.Print(" ")
	log.Printf("Количество файлов - %d Обрабатываем - %d\n", len(arFile), nSize)

	server := Cfg.GetValue("server", "192.168.101.1")
	user := "sa"
	password := "1551"
	//port := 1433
	database := "Analiz_EN"

	//	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
	//		server, user, password, port, database)
	//connString := makeConnURL(server, port, user, password).String()
	connString := fmt.Sprintf("odbc:server=%s;user id=%s;password=%s;database=%s;keepalive=0",
		server, user, password, database)
	//log.Print(connString)
	//	db, err = sql.Open("sqlserver", connString)
	connector, err := mssql.NewConnector(connString)
	if err != nil {
		log.Println(err)
		return
	}
	connector.SessionInitSQL = "SET ANSI_NULLS ON"

	db = sql.OpenDB(connector)
	defer db.Close()
	_, err = db.Exec("USE " + database)
	if err != nil {
		log.Println(err)
		return
	}

	for i := 0; i < nSize; i++ {
		if arFile[i].Mode().IsDir() {
			continue
		}
		iRet := ParseFile(db, arFile[i])
		if iRet == 0 {
			tmp := Cfg.GetValue("ScanDir", "")
			szFileName = arFile[i].Name()
			err = os.Remove(tmp + `\` + szFileName)
			if err != nil {
				log.Printf("Ошибка при удалении %s *** %s", szFileName, err.Error())
			}
		}
		if (iRet == 3) || (iRet == 1) {
			tmp := Cfg.GetValue("ScanDir", "")
			if tmp[len(tmp)-1] != '\\' {
				tmp = strings.Trim(tmp, " \t") + "\\"
			}
			szFileName1C := tmp + arFile[i].Name()
			tmp = Cfg.GetValue("ScanDir1C", "")
			if tmp[len(tmp)-1] != '\\' {
				tmp = strings.Trim(tmp, " \t") + "\\"
			}
			szFileName = arFile[i].Name()
			MyCopyFile(szFileName1C, tmp+szFileName)
		}
	}
}
