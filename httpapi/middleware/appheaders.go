package middleware

import (
	"context"
	"fmt"
	"net/http"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/httpapi/utils"
	"github.com/semafind/semadb/models"
)

type AppHeaders struct {
	UserId string `header:"X-User-Id" binding:"required"`
	PlanId string `header:"X-Plan-Id" binding:"required"`
}

type contextKey string

const appHeadersKey contextKey = "appHeaders"
const userPlanKey contextKey = "userPlan"

func AppHeaderMiddleware(userPlans map[string]models.UserPlan, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		appHeaders := AppHeaders{
			UserId: r.Header.Get("X-User-Id"),
			PlanId: r.Header.Get("X-Plan-Id"),
		}
		if appHeaders.UserId == "" || appHeaders.PlanId == "" {
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": "missing X-User-ID or X-Plan-Id headers"})
			return
		}
		log.Debug().Interface("appHeaders", appHeaders).Msg("AppHeaderMiddleware")
		// ---------------------------
		newCtx := context.WithValue(r.Context(), appHeadersKey, appHeaders)
		// Extract user plan
		userPlan, ok := userPlans[appHeaders.PlanId]
		if !ok {
			errmsg := fmt.Sprintf("unknown user plan %s", appHeaders.PlanId)
			utils.Encode(w, http.StatusBadRequest, map[string]string{"error": errmsg})
			return
		}
		newCtx = context.WithValue(newCtx, userPlanKey, userPlan)
		next.ServeHTTP(w, r.WithContext(newCtx))
	})
}

func GetAppHeaders(ctx context.Context) AppHeaders {
	return ctx.Value(appHeadersKey).(AppHeaders)
}

func GetUserPlan(ctx context.Context) models.UserPlan {
	return ctx.Value(userPlanKey).(models.UserPlan)
}
