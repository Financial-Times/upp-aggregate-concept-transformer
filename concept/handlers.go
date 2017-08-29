package concept

import (
	"net/http"

	"encoding/json"

	"fmt"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	ftstatus "github.com/Financial-Times/service-status-go/httphandlers"
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

	concordedConcept, transactionID, err, logMsg, status := h.svc.GetConcordedConcept(UUID)
	if err != nil {
		writeResponse(w, status, logMsg)
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

func writeResponse(rw http.ResponseWriter, updateStatus status, logMsg string) {
	rw.Header().Set("Content-Type", "application/json")
	switch updateStatus {
	case NOT_FOUND:
		rw.WriteHeader(http.StatusNotFound)
		fmt.Fprintln(rw, fmt.Sprintf("{\"message\": \"%s\"}", logMsg))
		return
	case DOWNSTREAM_ERROR:
		rw.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(rw, fmt.Sprintf("{\"message\": \"%s\"}", logMsg))
		return
	default:
		rw.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(rw, fmt.Sprintf("{\"message\": \"%s\"}", "An error has occured"))
		return
	}
}

func (h *AggregateConceptHandler) RegisterHandlers(router *mux.Router) {
	log.Info("Registering handlers")
	mh := handlers.MethodHandler{
		"GET": http.HandlerFunc(h.GetHandler),
	}
	sh := handlers.MethodHandler{
		"POST": http.HandlerFunc(h.SendHandler),
	}
	router.Handle("/concept/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}", mh)
	router.Handle("/concept/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}/send", sh)
}

func (h *AggregateConceptHandler) RegisterAdminHandlers(router *mux.Router, healthService *HealthService, requestLoggingEnabled bool) http.Handler {
	log.Info("Registering admin handlers")

	hc := fthealth.HealthCheck{SystemCode: healthService.config.appSystemCode, Name: healthService.config.appName, Description: healthService.config.description, Checks: healthService.Checks}
	router.HandleFunc("/__health", fthealth.Handler(hc))
	router.HandleFunc(ftstatus.GTGPath, ftstatus.NewGoodToGoHandler(healthService.GtgCheck))
	router.HandleFunc(ftstatus.BuildInfoPath, ftstatus.BuildInfoHandler)

	var monitoringRouter http.Handler = router
	if requestLoggingEnabled {
		monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	}
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)
	return monitoringRouter
}
