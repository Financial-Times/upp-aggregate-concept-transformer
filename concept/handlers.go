package concept

import (
	"net/http"

	"encoding/json"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
)

const appDescription = "Service to aggregate concepts from different sources and produce a canonical view."

type AggregateConceptHandler struct {
	svc Service
}

type httpClient interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

func NewHandler(svc Service) AggregateConceptHandler {
	return AggregateConceptHandler{svc: svc}
}

func (h *AggregateConceptHandler) GetHandler(rw http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	UUID := vars["uuid"]

	concordedConcept, transactionID, err := h.svc.GetConcordedConcept(UUID)
	if err != nil {
		//Do error stuff
	}

	rw.Header().Set("Content-Type", "application/json")
	rw.Header().Set("X-Request-Id", transactionID)
	rw.WriteHeader(http.StatusOK)
	json.NewEncoder(rw).Encode(concordedConcept)
}

func (h *AggregateConceptHandler) RegisterHandlers(router *mux.Router) {
	log.Info("Registering handlers")
	mh := handlers.MethodHandler{
		"GET": http.HandlerFunc(h.GetHandler),
	}
	router.Handle("/concept/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}", mh)
}

func (h *AggregateConceptHandler) RegisterAdminHandlers(router *mux.Router, healthService *HealthService) {
	log.Info("Registering admin handlers")

	hc := fthealth.HealthCheck{SystemCode: healthService.config.appSystemCode, Name: healthService.config.appName, Description: appDescription, Checks: healthService.Checks}
	router.HandleFunc("/__health", fthealth.Handler(hc))
	router.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(healthService.GtgCheck))
	router.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)

	var monitoringRouter http.Handler = router
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)
	http.Handle("/", monitoringRouter)
}
