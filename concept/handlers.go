package concept

import (
	"net/http"

	"encoding/json"

	"fmt"

	"time"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
)

type AggregateConceptHandler struct {
	svc Service
}

type httpClient interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

func NewHandler(svc Service) AggregateConceptHandler {
	return AggregateConceptHandler{svc: svc}
}

func (h *AggregateConceptHandler) GetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	UUID := vars["uuid"]
	w.Header().Set("Content-Type", "application/json")

	concordedConcept, transactionID, err := h.svc.GetConcordedConcept(UUID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, fmt.Sprintf("{\"message\": \"%s\"}", err.Error()))
		return
	}

	w.Header().Set("X-Request-Id", transactionID)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(concordedConcept)
}

func (h *AggregateConceptHandler) SendHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	UUID := vars["uuid"]
	w.Header().Set("Content-Type", "application/json")

	err := h.svc.ProcessMessage(UUID)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("{\"message\":\"Could not process the concept.\"}"))
		return
	}

	w.Write([]byte(fmt.Sprintf("{\"message\":\"Concept %s updated successfully.\"}", UUID)))
}

func (h *AggregateConceptHandler) RegisterHandlers(router *mux.Router) {
	logger.Info("Registering handlers")
	mh := handlers.MethodHandler{
		"GET": http.HandlerFunc(h.GetHandler),
	}
	sh := handlers.MethodHandler{
		"POST": http.HandlerFunc(h.SendHandler),
	}
	router.Handle("/concept/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}", mh)
	router.Handle("/concept/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}/send", sh)
}

func (h *AggregateConceptHandler) RegisterAdminHandlers(router *mux.Router, healthService *HealthService, requestLoggingEnabled bool, fb chan bool) http.Handler {
	logger.Info("Registering admin handlers")

	hc := fthealth.HealthCheck{
		SystemCode: healthService.config.appSystemCode,
		Name: healthService.config.appName,
		Description: healthService.config.description,
		Checks: healthService.Checks,
	}

	thc := fthealth.TimedHealthCheck{HealthCheck: hc, Timeout: 10 * time.Second}

	router.HandleFunc("/__health", fthealth.Handler(fthealth.NewFeedbackHealthCheck(thc, fb)))
	router.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(healthService.GTG))
	router.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)

	var monitoringRouter http.Handler = router
	if requestLoggingEnabled {
		monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	}
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)
	return monitoringRouter
}
