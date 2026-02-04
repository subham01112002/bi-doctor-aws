import React from 'react';
import { Check, Package } from 'lucide-react';
import '../css/ProgressBar.css';

const ProgressBar = ({ currentStage, logMessage, totalDatasources = 1 }) => {
  // Define all 6 stages based on your backend logger
  const stages = [
    { id: 1, name: 'Downloading Datasource', key: 'DOWNLOAD_DATASOURCE' },
    { id: 2, name: 'Publishing Datasource', key: 'PUBLISH_DATASOURCE' },
    { id: 3, name: 'Updating Connections', key: 'UPDATE_CONNECTIONS' },
    { id: 4, name: 'Downloading Workbook', key: 'DOWNLOAD_WORKBOOK' },
    { id: 5, name: 'Updating References', key: 'UPDATE_REFERENCES' },
    { id: 6, name: 'Publishing Workbook', key: 'PUBLISH_WORKBOOK' }
  ];

  // Calculate which datasource we're on and which step within the loop
  const getDatasourceProgress = () => {
    if (currentStage <= 0) return { current: 0, step: 0 };
    if (currentStage >= 100) return { current: totalDatasources, step: 6 };
    
    // Stages 1-3 loop for each datasource (each takes ~10 stages)
    // Stage 10 = start, 40 = after all datasources done
    const datasourcePhaseEnd = 10 + (totalDatasources * 10);
    
    if (currentStage < datasourcePhaseEnd) {
      // We're in the datasource loop phase
      const progressInLoop = currentStage - 10;
      const currentDs = Math.floor(progressInLoop / 10) + 1;
      const stepInDs = (progressInLoop % 10);
      
      let step = 1;
      if (stepInDs >= 7) step = 3; // Update connections
      else if (stepInDs >= 4) step = 2; // Publishing
      else step = 1; // Downloading
      
      return { current: Math.min(currentDs, totalDatasources), step };
    } else {
      // We're in the workbook phase (steps 4-6)
      const workbookProgress = currentStage - datasourcePhaseEnd;
      let step = 4;
      
      if (workbookProgress >= 20) step = 6; // Publishing workbook
      else if (workbookProgress >= 10) step = 5; // Updating references
      else step = 4; // Downloading workbook
      
      return { current: totalDatasources, step };
    }
  };

  const { current: currentDatasource, step: activeStep } = getDatasourceProgress();

  const getStepStatus = (stepId) => {
    if (stepId < activeStep) return 'completed';
    if (stepId === activeStep) return 'active';
    if (stepId <= 3 && activeStep <= 3) {
      // For datasource loop steps, show completed if we've done any iteration
      // if (currentDatasource > 1 && stepId <= 3) return 'completed';
      return 'pending';
    }
    return 'pending';
  };

  // Enhanced step label with datasource counter for steps 1-3
  const getStepLabel = (stage) => {
    if (stage.id <= 3 && activeStep <= 3 && totalDatasources > 1) {
      return `${stage.name} (${currentDatasource}/${totalDatasources})`;
    }
    return stage.name;
  };

  return (
    <div className="progress-overlay">
      <div className="progress-card">
        <h2 className="progress-title">Deployment in Progress</h2>
        
        <div className="stepper-wrapper">
          {stages.map((stage, index) => {
            const status = getStepStatus(stage.id);
            
            return (
              <React.Fragment key={stage.id}>
                <div className="step-item">
                  <div className="step-content">
                    <div className={`step-circle ${status}`}>
                      {status === 'completed' ? (
                        <Check size={20} strokeWidth={3} />
                      ) : status === 'active' ? (
                        <Package size={20} className="rotating" />
                      ) : (
                        stage.id
                      )}
                    </div>
                    <div className={`step-label ${status}`}>
                      {getStepLabel(stage)}
                    </div>
                  </div>
                </div>

                {index < stages.length - 1 && (
                  <div
                    className={`step-line ${
                      stage.id < activeStep ||
                      (stage.id <= 3 && activeStep <= 3 && activeStep > stage.id)
                        ? 'completed'
                        : ''
                    }`}
                  />
                )}
              </React.Fragment>
            );
          })}
        </div>

        {logMessage && (
          <div className="log-display">
            <strong>Status:</strong> {logMessage}
          </div>
        )}
      </div>
    </div>
  );
};

export default ProgressBar;