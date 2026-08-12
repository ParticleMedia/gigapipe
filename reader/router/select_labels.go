package router

import (
	"github.com/gorilla/mux"
	controllerv1 "github.com/metrico/qryn/v5/reader/controller"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/service"
	"github.com/metrico/qryn/v5/reader/utils/stat"
)

func RouteSelectLabels(app *mux.Router, dataSession model.IDBRegistry) {
	qrService := service.NewQueryLabelsService(&model.ServiceData{
		Session: dataSession,
	})
	qrCtrl := &controllerv1.QueryLabelsController{
		QueryLabelsService: qrService,
	}
	app.HandleFunc("/loki/api/v1/label", qrCtrl.Labels).Methods("GET", "POST", "OPTIONS")
	app.HandleFunc("/loki/api/v1/labels", stat.InstrumentRoute("gigapipe_loki_api_v1_labels", qrCtrl.Labels)).Methods("GET", "POST", "OPTIONS")
	app.HandleFunc("/loki/api/v1/label/{name}/values", qrCtrl.Values).Methods("GET", "POST", "OPTIONS")
	app.HandleFunc("/loki/api/v1/series", stat.InstrumentRoute("gigapipe_loki_api_v1_series", qrCtrl.Series)).Methods("GET", "POST", "OPTIONS")
}
