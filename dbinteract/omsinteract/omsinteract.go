package omsinteract

import (
	"database/sql"
	"log"

	"github.com/thomas-bamilo/financebooking/row/scomsrow"
)

// GetOmsData gets the OMS data required for Finance Booking process
// filtered for omsIDSalesOrderItemList found in Seller Center data
func GetOmsData(dbOms *sql.DB, omsIDSalesOrderItemList string) []scomsrow.ScOmsRow {

	// store LedgerMapKeyQuery in a string
	LedgerMapKeyQuery := `
	SELECT
	isoi.id_sales_order_item
	,isois.name 'item_status'
	,iso.payment_method 'payment_method'
	,osp.shipment_provider_name 'shipment_provider_name'
	,isoi.paid_price 'paid_price'
  
	FROM ims_sales_order_item isoi
  
	LEFT JOIN ims_sales_order iso
	ON iso.id_sales_order = isoi.fk_sales_order
  
	LEFT JOIN oms_package_item opi
	ON opi.fk_sales_order_item = isoi.id_sales_order_item
  
	LEFT JOIN oms_package_dispatching opd
	ON opi.fk_package = opd.fk_package
  
	LEFT JOIN oms_shipment_provider osp
	ON opd.fk_shipment_provider = osp.id_shipment_provider
  
	LEFT JOIN ims_sales_order_item_status isois
	ON isois.id_sales_order_item_status = isoi.fk_sales_order_item_status

	WHERE isoi.id_sales_order_item IN(` + omsIDSalesOrderItemList + `)
	
	GROUP BY isoi.id_sales_order_item`

	// write LedgerMapKeyQuery result to an array of scomsrow.ScOmsRow , this array of rows represents omsTable
	var itemStatus, paymentMethod, shipmentProviderName string
	var omsIDSalesOrderItem int
	var paidPrice float32

	var omsTable []scomsrow.ScOmsRow

	rows, _ := dbOms.Query(LedgerMapKeyQuery)

	for rows.Next() {
		err := rows.Scan(&omsIDSalesOrderItem, &itemStatus, &paymentMethod, &shipmentProviderName, &paidPrice)
		checkError(err)
		omsTable = append(omsTable,
			scomsrow.ScOmsRow{
				OmsIDSalesOrderItem:  omsIDSalesOrderItem,
				ItemStatus:           itemStatus,
				PaymentMethod:        paymentMethod,
				ShipmentProviderName: shipmentProviderName,
				PaidPrice:            paidPrice,
			})
	}

	return omsTable
}

/*// GetLedgerMapKey gets all the existing id_seller_rejection from baa_application.baa_application_schema.seller_rejection and store then into an array of scomsrow.ScOmsRow
func GetLedgerMapKey(dbOms *sql.DB, omsIDSalesOrderItemList string) []scomsrow.ScOmsRow {

	// store LedgerMapKeyQuery in a string
	LedgerMapKeyQuery := `
	SELECT DISTINCT

	CONCAT(isois.name,'-', iso.payment_method,'-',osp.shipment_provider_name) 'ledger_map_key'

	FROM ims_sales_order_item isoi

	LEFT JOIN ims_sales_order iso
	ON isoi.fk_sales_order = iso.id_sales_order

	LEFT JOIN oms_package_item opi
	ON opi.fk_sales_order_item = isoi.id_sales_order_item

	LEFT JOIN oms_package_dispatching opd
	ON opi.fk_package = opd.fk_package

	LEFT JOIN oms_shipment_provider osp
	ON opd.fk_shipment_provider = osp.id_shipment_provider

	LEFT JOIN ims_sales_order_item_status isois
	ON isois.id_sales_order_item_status = isoi.fk_sales_order_item_status

	LEFT JOIN
	ims_sales_order_item_status_history AS shipped_at
	ON isoi.id_sales_order_item = shipped_at.fk_sales_order_item
	AND shipped_at.fk_sales_order_item_status='5'

	WHERE shipped_at.created_at IS NOT NULL
	AND isoi.id_sales_order_item IN (` + omsIDSalesOrderItemList + `) `

	// write LedgerMapKeyQuery result to an array of scomsrow.ScOmsRow , this array of rows represents omsTable
	var ledgerMapKey string
	var omsTable []scomsrow.ScOmsRow

	rows, _ := dbOms.Query(LedgerMapKeyQuery)

	for rows.Next() {
		err := rows.Scan(&ledgerMapKey)
		checkError(err)
		omsTable = append(omsTable,
			scomsrow.ScOmsRow{
				LedgerMapKey: ledgerMapKey,
			})
	}

	return omsTable
}*/

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
