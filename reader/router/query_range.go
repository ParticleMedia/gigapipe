package router

import (
	"github.com/gorilla/mux"
	"github.com/metrico/qryn/v5/reader/config"
	controllerv1 "github.com/metrico/qryn/v5/reader/controller"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/service"
	"github.com/metrico/qryn/v5/reader/utils/stat"
)

func RouteQueryRangeApis(app *mux.Router, dataSession model.IDBRegistry) {
	qrService := &service.QueryRangeService{
		ServiceData: model.ServiceData{
			Session: dataSession,
		},
	}
	qrCtrl := &controllerv1.QueryRangeController{
		QueryRangeService: qrService,
	}
	app.HandleFunc("/loki/api/v1/query_range", stat.InstrumentRoute("gigapipe_loki_api_v1_query_range", qrCtrl.QueryRange)).Methods("GET", "OPTIONS")
	app.HandleFunc("/loki/api/v1/query", stat.InstrumentRoute("gigapipe_loki_api_v1_query", qrCtrl.Query)).Methods("GET", "OPTIONS")
	app.HandleFunc("/loki/api/v1/tail", qrCtrl.Tail).Methods("GET", "OPTIONS")
	app.HandleFunc("/loki/api/v1/index/stats", qrCtrl.IndexStats).Methods("GET", "OPTIONS")

	if config.Cloki.Setting.DRILLDOWN_SETTINGS.LogDrilldown {
		vCtrl := &controllerv1.VolumeController{
			QueryRangeService: qrService,
		}
		app.HandleFunc("/loki/api/v1/index/volume", vCtrl.Volume).Methods("GET", "OPTIONS")
		app.HandleFunc("/loki/api/v1/detected_labels", vCtrl.DetectedLabels).Methods("GET", "OPTIONS")
		app.HandleFunc("/loki/api/v1/detected_fields", vCtrl.DetectedFields).Methods("GET", "OPTIONS")
		app.HandleFunc("/loki/api/v1/patterns", vCtrl.Patterns).Methods("GET", "OPTIONS")
	}
}
