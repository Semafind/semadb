package middleware

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/models"
)

type AppHeaders struct {
	UserId string `header:"X-User-Id" binding:"required"`
	PlanId string `header:"X-Plan-Id" binding:"required"`
}

func AppHeaderMiddleware(userPlans map[string]models.UserPlan) gin.HandlerFunc {
	return func(c *gin.Context) {
		var appHeaders AppHeaders
		if err := c.ShouldBindHeader(&appHeaders); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.Set("appHeaders", appHeaders)
		log.Debug().Interface("appHeaders", appHeaders).Msg("AppHeaderMiddleware")
		// Extract user plan
		userPlan, ok := userPlans[appHeaders.PlanId]
		if !ok {
			errmsg := fmt.Sprintf("unknown user plan %s", appHeaders.PlanId)
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": errmsg})
			return
		}
		c.Set("userPlan", userPlan)
		c.Next()
	}
}
