package concept

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	status "github.com/Financial-Times/service-status-go/httphandlers"
)

type AggregateConceptHandler struct {
	svc            Service
	requestTimeout time.Duration
}

type httpClient interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

func NewHandler(svc Service, timeout time.Duration) AggregateConceptHandler {
	return AggregateConceptHandler{svc: svc, requestTimeout: timeout}
}

func (h *AggregateConceptHandler) GetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	UUID := vars["uuid"]
	w.Header().Set("Content-Type", "application/json")
	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimeout)
	defer cancel()

	concept, transactionID, err := h.getConcordedConcept(ctx, UUID)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "{\"message\":\"%v\"}", err)
		return
	}

	w.Header().Set("X-Request-Id", transactionID)
	w.WriteHeader(http.StatusOK)
	//nolint:errcheck
	json.NewEncoder(w).Encode(concept)
}

func (h *AggregateConceptHandler) getConcordedConcept(ctx context.Context, UUID string) (ConcordedConcept, string, error) {

	type concordedTransaction struct {
		Concept       ConcordedConcept
		TransactionID string
		Err           error
	}

	transaction := make(chan concordedTransaction)
	var data concordedTransaction

	go func() {
		concordedConcept, transactionID, err := h.svc.GetConcordedConcept(ctx, UUID, "")
		transaction <- concordedTransaction{Concept: concordedConcept, TransactionID: transactionID, Err: err}
	}()

	select {
	case data = <-transaction:
	case <-ctx.Done():
		data.Err = ctx.Err()
	}

	return data.Concept, data.TransactionID, data.Err
}

func (h *AggregateConceptHandler) SendHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	UUID := vars["uuid"]
	w.Header().Set("Content-Type", "application/json")

	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimeout)
	defer cancel()

	ch := make(chan error)
	go func() {
		err := h.svc.ProcessMessage(ctx, UUID, "")
		ch <- err
	}()
	var err error
	select {
	case err = <-ch:
	case <-ctx.Done():
		err = ctx.Err()
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "{\"message\":\"%v\"}", err)
		return
	}
	//nolint:errcheck
	w.Write([]byte(fmt.Sprintf("{\"message\":\"Concept %s updated successfully.\"}", UUID)))
}

func (h *AggregateConceptHandler) RegisterHandlers(healthService *HealthService, requestLoggingEnabled bool, fb chan bool) *http.ServeMux {
	logger.Info("Registering handlers")

	router := mux.NewRouter()
	mh := handlers.MethodHandler{
		"GET": http.HandlerFunc(h.GetHandler),
	}
	sh := handlers.MethodHandler{
		"POST": http.HandlerFunc(h.SendHandler),
	}
	router.Handle("/concept/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}", mh)
	router.Handle("/concept/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}/send", sh)

	var monitoringRouter http.Handler = router
	if requestLoggingEnabled {
		monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	}
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	logger.Info("Registering admin handlers")

	hc := fthealth.HealthCheck{
		SystemCode:  healthService.config.appSystemCode,
		Name:        healthService.config.appName,
		Description: healthService.config.description,
		Checks:      healthService.Checks,
	}

	thc := fthealth.TimedHealthCheck{HealthCheck: hc, Timeout: 10 * time.Second}

	serveMux := http.NewServeMux()
	serveMux.HandleFunc("/__health", fthealth.Handler(fthealth.NewFeedbackHealthCheck(thc, fb)))
	serveMux.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(healthService.GTG))
	serveMux.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)
	serveMux.Handle("/", monitoringRouter)

	return serveMux
}
