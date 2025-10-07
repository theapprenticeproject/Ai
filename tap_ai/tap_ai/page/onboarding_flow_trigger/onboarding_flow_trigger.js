frappe.pages['onboarding-flow-trigger'].on_page_load = function(wrapper) {
    var page = frappe.ui.make_app_page({
        parent: wrapper,
        title: 'Onboarding Flow Trigger',
        single_column: true
    });

    // Initialize the page controller
    new OnboardingFlowTrigger(page);
};

class OnboardingFlowTrigger {
    constructor(page) {
        this.page = page;
        this.wrapper = $(page.body);
        this.filters = {};
        this.setup_page();
    }

    setup_page() {
        // Render the page HTML
        $(frappe.render_template('onboarding_flow_trigger', {})).appendTo(this.wrapper);
        
        // Initialize form fields
        this.setup_filters();
        
        // Set up event listeners
        this.setup_events();
        
        // Initialize the report section
        this.setup_report_section();
    }

    setup_filters() {
        // Backend Student Onboarding Set filter
        this.filters.onboarding_set = frappe.ui.form.make_control({
            parent: this.wrapper.find('.onboarding-set-field'),
            df: {
                fieldtype: 'Link',
                options: 'Backend Student Onboarding',
                fieldname: 'onboarding_set',
                label: 'Backend Student Onboarding Set',
                filters: {
                    'status': 'Processed'
                },
                placeholder: 'Select Backend Student Onboarding Set',
                reqd: 1,
                get_query: function() {
                    return {
                        filters: {
                            'status': 'Processed'
                        }
                    };
                }
            },
            render_input: true
        });
        
        // Onboarding Stage filter
        this.filters.onboarding_stage = frappe.ui.form.make_control({
            parent: this.wrapper.find('.onboarding-stage-field'),
            df: {
                fieldtype: 'Link',
                options: 'OnboardingStage',
                fieldname: 'onboarding_stage',
                label: 'Onboarding Stage',
                placeholder: 'Select Onboarding Stage',
                filters: {
                    'is_active': 1
                },
                reqd: 1,
                get_query: function() {
                    return {
                        filters: {
                            'is_active': 1
                        }
                    };
                },
                onchange: () => {
                    // When stage changes, update available statuses
                    this.update_status_options();
                }
            },
            render_input: true
        });
        
        // Add Status filter
        // First, add a container for the status field
        this.wrapper.find('.onboarding-stage-field').after('<div class="student-status-field"></div>');
        
        // Then create the status select field
        this.filters.student_status = frappe.ui.form.make_control({
            parent: this.wrapper.find('.student-status-field'),
            df: {
                fieldtype: 'Select',
                fieldname: 'student_status',
                label: 'Student Status Filter',
                options: '\nnot_started\nassigned\nin_progress\ncompleted\nincomplete\nskipped',
                default: '',
                description: 'Only trigger flows for students in this status',
                reqd: 1
            },
            render_input: true
        });
    }
    
    update_status_options() {
        const stage_id = this.filters.onboarding_stage.get_value();
        if (!stage_id) {
            return;
        }
        
        // Clear current status value
        this.filters.student_status.set_value('');
        
        // Show loading
        this.filters.student_status.set_description('Loading available statuses...');
        
        // Fetch available flows for this stage
        frappe.call({
            method: 'tap_ai.tap_ai.page.onboarding_flow_trigger.onboarding_flow_trigger.get_stage_flow_statuses',
            args: {
                'stage_id': stage_id
            },
            callback: (r) => {
                if (r.message && r.message.statuses && r.message.statuses.length > 0) {
                    // Update the status dropdown options
                    let options = '\n' + r.message.statuses.join('\n');
                    this.filters.student_status.df.options = options;
                    this.filters.student_status.refresh();
                    
                    // Set a default if available
                    this.filters.student_status.set_value(r.message.statuses[0]);
                    this.filters.student_status.set_description('Only trigger flows for students in this status');
                } else {
                    // No flows configured
                    this.filters.student_status.df.options = '\n';
                    this.filters.student_status.refresh();
                    this.filters.student_status.set_description('No flows configured for this stage');
                }
            },
            error: (r) => {
                this.filters.student_status.set_description('Error loading statuses');
            }
        });
    }
    
    setup_events() {
        // Trigger flow button click event
        this.wrapper.find('.trigger-flow-btn').on('click', () => {
            this.trigger_flow();
        });
        
        // Refresh report button click event
        this.wrapper.find('.refresh-report-btn').on('click', () => {
            this.refresh_report();
        });
    }
    
    setup_report_section() {
        // Set up report filter fields
        this.filters.report_set = frappe.ui.form.make_control({
            parent: this.wrapper.find('.report-set-field'),
            df: {
                fieldtype: 'Link',
                options: 'Backend Student Onboarding',
                fieldname: 'report_filter_set',
                label: 'Onboarding Set',
                placeholder: 'All Sets'
            },
            render_input: true
        });
        
        this.filters.report_stage = frappe.ui.form.make_control({
            parent: this.wrapper.find('.report-stage-field'),
            df: {
                fieldtype: 'Link',
                options: 'OnboardingStage',
                fieldname: 'report_filter_stage',
                label: 'Onboarding Stage',
                placeholder: 'All Stages',
                get_query: function() {
                    return {
                        filters: {
                            'is_active': 1
                        }
                    };
                }
            },
            render_input: true
        });
        
        // Add Status filter for report
        this.wrapper.find('.report-stage-field').after('<div class="report-status-field"></div>');
        
        this.filters.report_status = frappe.ui.form.make_control({
            parent: this.wrapper.find('.report-status-field'),
            df: {
                fieldtype: 'Select',
                fieldname: 'report_filter_status',
                label: 'Status',
                options: '\nnot_started\nassigned\nin_progress\ncompleted\nincomplete\nskipped',
                placeholder: 'All Statuses'
            },
            render_input: true
        });
        
        // Add a direct report button for testing
        this.wrapper.find('.filter-area').append(`
            <div class="form-group mt-2">
                <button class="btn btn-info test-direct-report-btn">Test Direct Report</button>
            </div>
        `);
        
        // Add event handler for the direct report button
        this.wrapper.find('.test-direct-report-btn').on('click', () => {
            const set = this.filters.report_set.get_value();
            const stage = this.filters.report_stage.get_value();
            
            // Call the report function directly for testing
            frappe.call({
                method: 'frappe.client.get_list',
                args: {
                    doctype: 'StudentStageProgress',
                    filters: {
                        'stage_type': 'OnboardingStage'
                    },
                    fields: ['name', 'student', 'stage', 'status'],
                    limit: 5
                },
                callback: (r) => {
                    if (r.message) {
                        frappe.msgprint({
                            title: 'Test Report Results',
                            message: `Found ${r.message.length} records: <br><pre>${JSON.stringify(r.message, null, 2)}</pre>`,
                            indicator: 'green'
                        });
                    }
                }
            });
        });
        
        // Load initial report data
        setTimeout(() => {
            this.refresh_report();
        }, 1000);
    }
    
    trigger_flow() {
        const onboarding_set = this.filters.onboarding_set.get_value();
        const onboarding_stage = this.filters.onboarding_stage.get_value();
        const student_status = this.filters.student_status.get_value();
        
        if (!onboarding_set || !onboarding_stage) {
            frappe.msgprint(__('Please select both Backend Student Onboarding Set and Onboarding Stage'));
            return;
        }
        
        if (!student_status) {
            frappe.msgprint(__('Please select a Student Status'));
            return;
        }
        
        // Show loading state
        this.wrapper.find('.trigger-flow-btn').prop('disabled', true).html('<i class="fa fa-spinner fa-spin"></i> Processing...');
        this.wrapper.find('.processing-log').html('<div class="alert alert-info">Initiating flow trigger process...</div>');
        
        // Call the server method to trigger the flow with status filter
        frappe.call({
            method: 'tap_ai.tap_ai.page.onboarding_flow_trigger.onboarding_flow_trigger.trigger_onboarding_flow',
            args: {
                'onboarding_set': onboarding_set,
                'onboarding_stage': onboarding_stage,
                'student_status': student_status
            },
            freeze: true,
            freeze_message: __('Triggering Flow...'),
            callback: (r) => {
                this.wrapper.find('.trigger-flow-btn').prop('disabled', false).html('Trigger Flow');
                console.log("Response:", r);
                
                if (r.message) {
                    if (r.message.job_id) {
                        // If background job was started
                        this.wrapper.find('.processing-log').html(
                            `<div class="alert alert-success">Flow trigger process started in the background.</div>`
                        );
                        this.wrapper.find('.job-status-container').html(
                            `<div class="job-status mt-2">
                                <p>Job ID: ${r.message.job_id}</p>
                                <p>Background job started. You'll be notified when completed.</p>
                                <small>You can close this page and check the status later.</small>
                            </div>`
                        );
                        
                        // Check job status periodically
                        this.check_job_status(r.message.job_id);
                    } else if (r.message.error) {
                        // Error in response
                        this.wrapper.find('.processing-log').html(
                            `<div class="alert alert-danger">Error: ${r.message.error}</div>`
                        );
                    } else {
                        // Direct response without background job
                        let status_html = '<div class="alert alert-success">Flow triggered successfully!</div>';
                        status_html += '<div class="results-details mt-2">';
                        
                        if (r.message.group_flow_result) {
                            status_html += `<p>Group flow triggered for ${r.message.group_count || 0} students</p>`;
                        }
                        
                        if (r.message.individual_flow_results) {
                            status_html += `<p>Individual flows triggered for ${r.message.individual_count || 0} students</p>`;
                        }
                        
                        status_html += '</div>';
                        this.wrapper.find('.processing-log').html(status_html);
                    }
                    
                    // Set the report filters to match the flow that was just triggered
                    this.filters.report_set.set_value(onboarding_set);
                    this.filters.report_stage.set_value(onboarding_stage);
                    this.filters.report_status.set_value(student_status);
                    
                    // Refresh report after a short delay
                    setTimeout(() => {
                        this.refresh_report();
                    }, 2000);
                } else {
                    // No message in response
                    this.wrapper.find('.processing-log').html(
                        '<div class="alert alert-warning">Flow triggered, but no response details received.</div>'
                    );
                }
            },
            error: (xhr, textStatus, errorThrown) => {
                this.wrapper.find('.trigger-flow-btn').prop('disabled', false).html('Trigger Flow');
                
                console.error("Error details:", {
                    xhr: xhr,
                    status: textStatus,
                    error: errorThrown,
                    responseText: xhr.responseText,
                    responseJSON: xhr.responseJSON
                });
                
                let errorMessage = 'Error triggering flow: ';
                
                if (xhr.responseJSON && xhr.responseJSON._server_messages) {
                    try {
                        // Parse server messages if they exist
                        const messages = JSON.parse(xhr.responseJSON._server_messages);
                        errorMessage += messages.join(', ');
                    } catch (e) {
                        errorMessage += xhr.responseJSON._server_messages;
                    }
                } else if (xhr.responseText && xhr.responseText.includes('<pre>')) {
                    // Extract error from HTML error page
                    try {
                        const errorMatch = xhr.responseText.match(/<pre>([^<]*)<\/pre>/);
                        if (errorMatch && errorMatch[1]) {
                            errorMessage += errorMatch[1].trim();
                        } else {
                            errorMessage += 'Server error (500)';
                        }
                    } catch (e) {
                        errorMessage += 'Error parsing server response';
                    }
                } else {
                    errorMessage += errorThrown || textStatus || 'Unknown error';
                }
                
                this.wrapper.find('.processing-log').html(
                    `<div class="alert alert-danger">${errorMessage}</div>`
                );
            }
        });
    }
    
    check_job_status(job_id) {
        if (!job_id) return;
        
        console.log("Checking job status for:", job_id);
        
        const status_checker = setInterval(() => {
            frappe.call({
                method: 'tap_ai.tap_ai.page.onboarding_flow_trigger.onboarding_flow_trigger.get_job_status',
                args: {
                    'job_id': job_id
                },
                callback: (r) => {
                    console.log("Job status response:", r);
                    
                    if (r.message) {
                        const status = r.message.status;
                        const results = r.message.results || {};
                        
                        // Update status display
                        let status_html = `<div class="alert alert-${status === 'complete' ? 'success' : (status === 'failed' ? 'danger' : 'info')}">
                            Job Status: ${status}
                        </div>`;
                        
                        if (status === 'complete' || status === 'failed') {
                            clearInterval(status_checker);
                            
                            if (status === 'complete' && results) {
                                status_html += '<div class="results-details mt-2">';
                                
                                if (results.group_flow_result) {
                                    status_html += `<p>Group flow triggered for ${results.group_count || 0} students</p>`;
                                }
                                
                                if (results.individual_flow_results) {
                                    status_html += `<p>Individual flows triggered for ${results.individual_count || 0} students</p>`;
                                }
                                
                                if (results.error) {
                                    status_html += `<p>Error: ${results.error}</p>`;
                                }
                                
                                status_html += '</div>';
                            }
                            
                            // Refresh report after job completion
                            setTimeout(() => {
                                this.refresh_report();
                            }, 1000);
                        }
                        
                        this.wrapper.find('.job-status-container').html(
                            `<div class="job-status mt-2">
                                <p>Job ID: ${job_id}</p>
                                ${status_html}
                            </div>`
                        );
                    } else {
                        console.error("No message in job status response");
                    }
                },
                error: (err) => {
                    console.error("Error checking job status:", err);
                    this.wrapper.find('.job-status-container').html(
                        `<div class="job-status mt-2">
                            <p>Job ID: ${job_id}</p>
                            <div class="alert alert-danger">Error checking job status</div>
                        </div>`
                    );
                }
            });
        }, 5000);  // Check every 5 seconds
        
        // Store the interval ID to clear it when needed
        this.status_checker_interval = status_checker;
    }
    
    refresh_report() {
        const set = this.filters.report_set.get_value();
        const stage = this.filters.report_stage.get_value();
        const status = this.filters.report_status.get_value();
        
        this.wrapper.find('.refresh-report-btn').prop('disabled', true).html('<i class="fa fa-spinner fa-spin"></i> Loading...');
        
        frappe.call({
            method: 'tap_ai.tap_ai.page.onboarding_flow_trigger.onboarding_flow_trigger.get_onboarding_progress_report',
            args: {
                'set': set,
                'stage': stage,
                'status': status
            },
            callback: (r) => {
                this.wrapper.find('.refresh-report-btn').prop('disabled', false).html('Refresh Report');
                
                if (r.message) {
                    const report_data = r.message;
                    
                    // Render summary statistics
                    this.render_report_summary(report_data.summary);
                    
                    // Render detailed report table
                    this.render_report_table(report_data.details);
                }
            },
            error: (xhr, textStatus, errorThrown) => {
                this.wrapper.find('.refresh-report-btn').prop('disabled', false).html('Refresh Report');
                
                console.error("Report error details:", {
                    xhr: xhr,
                    status: textStatus,
                    error: errorThrown
                });
                
                let errorMessage = 'Error loading report: ';
                
                if (xhr.responseJSON && xhr.responseJSON._server_messages) {
                    try {
                        const messages = JSON.parse(xhr.responseJSON._server_messages);
                        errorMessage += messages.join(', ');
                    } catch (e) {
                        errorMessage += xhr.responseJSON._server_messages;
                    }
                } else if (xhr.responseText && xhr.responseText.includes('<pre>')) {
                    try {
                        const errorMatch = xhr.responseText.match(/<pre>([^<]*)<\/pre>/);
                        if (errorMatch && errorMatch[1]) {
                            errorMessage += errorMatch[1].trim();
                        } else {
                            errorMessage += 'Server error (500)';
                        }
                    } catch (e) {
                        errorMessage += 'Error parsing server response';
                    }
                } else {
                    errorMessage += errorThrown || textStatus || 'Unknown error';
                }
                
                this.wrapper.find('.report-container').html(
                    `<div class="alert alert-danger">${errorMessage}</div>`
                );
            }
        });
    }
    
    render_report_summary(summary) {
        if (!summary) return;
        
        const summary_html = `
            <div class="row stats-cards">
                <div class="col-md-2">
                    <div class="card text-center">
                        <div class="card-body">
                            <h5 class="card-title">${summary.total || 0}</h5>
                            <p class="card-text">Total</p>
                        </div>
                    </div>
                </div>
                <div class="col-md-2">
                    <div class="card text-center bg-light">
                        <div class="card-body">
                            <h5 class="card-title">${summary.not_started || 0}</h5>
                            <p class="card-text">Not Started</p>
                        </div>
                    </div>
                </div>
                <div class="col-md-2">
                    <div class="card text-center bg-info text-white">
                        <div class="card-body">
                            <h5 class="card-title">${summary.assigned || 0}</h5>
                            <p class="card-text">Assigned</p>
                        </div>
                    </div>
                </div>
                <div class="col-md-2">
                    <div class="card text-center bg-warning text-white">
                        <div class="card-body">
                            <h5 class="card-title">${summary.in_progress || 0}</h5>
                            <p class="card-text">In Progress</p>
                        </div>
                    </div>
                </div>
                <div class="col-md-2">
                    <div class="card text-center bg-success text-white">
                        <div class="card-body">
                            <h5 class="card-title">${summary.completed || 0}</h5>
                            <p class="card-text">Completed</p>
                        </div>
                    </div>
                </div>
                <div class="col-md-2">
                    <div class="card text-center bg-danger text-white">
                        <div class="card-body">
                            <h5 class="card-title">${summary.incomplete || 0}</h5>
                            <p class="card-text">Incomplete</p>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        this.wrapper.find('.report-summary').html(summary_html);
    }
    
    render_report_table(details) {
        if (!details || !details.length) {
            this.wrapper.find('.report-table').html('<div class="alert alert-info">No data found for the selected filters.</div>');
            return;
        }
        
        // Create a DataTable for the detailed report
        const columns = [
            { name: 'Student Name', field: 'student_name', width: 150 },
            { name: 'Phone', field: 'phone', width: 120 },
            { name: 'Stage', field: 'stage', width: 120 },
            { name: 'Status', field: 'status', width: 100, 
              format: (value) => {
                  // Fix for undefined value
                  if (!value) return '<span class="badge bg-light">Not Available</span>';
                  
                  const status_colors = {
                      'not_started': 'light',
                      'assigned': 'info',
                      'in_progress': 'warning',
                      'completed': 'success',
                      'incomplete': 'danger',
                      'skipped': 'secondary'
                  };
                  const color = status_colors[value] || 'light';
                  return `<span class="badge bg-${color}">${frappe.utils.title_case(value.replace('_', ' '))}</span>`;
              }
            },
            { name: 'Start Date', field: 'start_timestamp', width: 120,
              format: (value) => value ? frappe.datetime.str_to_user(value) : '-'
            },
            { name: 'Last Activity', field: 'last_activity_timestamp', width: 120,
              format: (value) => value ? frappe.datetime.str_to_user(value) : '-'
            },
            { name: 'Completion Date', field: 'completion_timestamp', width: 120,
              format: (value) => value ? frappe.datetime.str_to_user(value) : '-'
            }
        ];
        
        const options = {
            columns: columns,
            data: details,
            layout: 'fixed'
        };
        
        // Clear previous table if exists
        this.wrapper.find('.report-table').empty();
        
        // Create new datatable
        const datatable = new frappe.DataTable(
            this.wrapper.find('.report-table').get(0),
            options
        );
        
        // Store reference to datatable
        this.report_datatable = datatable;
    }
}
