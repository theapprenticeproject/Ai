frappe.pages['backend_onboarding_process'].on_page_load = function(wrapper) {
    try {
        console.log("Loading Backend Onboarding Process page...");
        var page = frappe.ui.make_app_page({
            parent: wrapper,
            title: __('Backend Student Onboarding'),
            single_column: true
        });
        
        // Add CSS
        frappe.dom.set_style(`
            .backend-onboarding-container {
                padding: 15px;
            }
            .batch-selection-panel,
            .student-preview,
            .action-panel,
            .results-panel {
                margin-bottom: 20px;
            }
            .hidden {
                display: none;
            }
            .status-indicator .indicator {
                margin-right: 5px;
            }
            .batch-summary {
                margin-top: 15px;
                padding: 15px;
                background-color: #f9f9f9;
                border-radius: 5px;
            }
            .metrics div {
                margin-bottom: 5px;
            }
            .validation-cell .indicator {
                display: block;
                margin-bottom: 5px;
            }
            .progress-container {
                margin-top: 10px;
            }
        `);
        
        // Initialize page with controller
        try {
            new BackendOnboardingProcess(page);
            console.log("Backend Onboarding Process initialized successfully");
        } catch (e) {
            console.error("Error initializing BackendOnboardingProcess:", e);
            $(page.body).html(`
                <div class="alert alert-danger">
                    <h4>Error Initializing Page</h4>
                    <p>Error: ${e.message}</p>
                    <p>Please check the console for more details.</p>
                </div>
            `);
        }
    } catch (e) {
        console.error("Error in page_load:", e);
        $(wrapper).html(`
            <div class="alert alert-danger">
                <h4>Error Loading Page</h4>
                <p>Error: ${e.message}</p>
                <p>Please check the console for more details.</p>
            </div>
        `);
    }
};

class BackendOnboardingProcess {
    constructor(page) {
        this.page = page;
        this.wrapper = $(page.body);
        this.setup();
        this.load_data();
    }
    
    setup() {
                        /* <div class="student-preview hidden">
                    <h3>Student Records <span class="count"></span></h3>
                    <div class="table-responsive">
                        <table class="table table-bordered">
                            <thead>
                                <tr>
                                    <th>Status</th>
                                    <th>Name</th>
                                    <th>Phone</th>
                                    <th>Gender</th>
                                    <th>School</th>
                                    <th>Grade</th>
                                    <th>Course</th>
                                    <th>Batch</th>
                                    <th>Language</th>
                                    <th>Validation</th>
                                </tr>
                            </thead>
                            <tbody></tbody>
                        </table>
                    </div>
                </div> */
        console.log("Loading template directly...");
                            /* <div class="row">
                        <div class="col-md-6">
                            <div class="checkbox">
                                <label>
                                    <input type="checkbox" name="background_job" checked>
                                    Process as background job (recommended for large batches)
                                </label>
                            </div>
                        </div>
                    </div> */
        // Directly set HTML content with simplified UI
        this.wrapper.html(`
            <div class="backend-onboarding-container">
                <div class="batch-selection-panel">
                    <h3>Select Onboarding Set</h3>
                    <div class="filters">
                        <select class="batch-selector form-control"></select>
                    </div>
                    
                    <div class="batch-summary hidden">
                        <div class="status-indicator"></div>
                        <div class="metrics"></div>
                    </div>
                </div>
                

                
                <div class="action-panel hidden">
                    <div class="alert alert-info">
                        <p><strong>Note:</strong> Processing will:</p>
                        <ul>
                            <li>Create student records in Frappe</li>
                            <li>Create contacts in Glific (if integrated)</li>
                            <li>Update existing students with matching phone number and name</li>
                        </ul>
                        
                    </div>
                    

                    <button class="process-btn btn btn-primary">Start Processing</button>
                    <button class="cancel-btn btn btn-default">Cancel</button>
                </div>
                
                <div class="results-panel hidden">
                    <h3>Processing Results</h3>
                    
                    <div class="progress">
                        <div class="progress-bar" role="progressbar" style="width: 0%"></div>
                    </div>
                    
                    <div class="counts">
                        <span>Processed: <b class="processed-count">0/0</b></span>
                        <span>Success: <b class="success-count">0</b></span>
                        <span>Failed: <b class="failed-count">0</b></span>
                    </div>
                    
                    <div class="error-log hidden">
                        <h4>Errors</h4>
                        <div class="table-responsive">
                            <table class="table table-bordered">
                                <thead>
                                    <tr>
                                        <th>Student</th>
                                        <th>Error</th>
                                    </tr>
                                </thead>
                                <tbody></tbody>
                            </table>
                        </div>
                        <button class="export-errors btn btn-default">Export Error Log</button>
                    </div>
                    
                    <div class="success-log hidden">
                        <h4>Successfully Created Students</h4>
                        <div class="table-responsive">
                            <table class="table table-bordered">
                                <thead>
                                    <tr>
                                        <th>Name</th>
                                        <th>Phone</th>
                                        <th>Student ID</th>
                                        <th>Glific ID</th>
                                    </tr>
                                </thead>
                                <tbody></tbody>
                            </table>
                        </div>
                        <button class="export-results btn btn-default">Export Results</button>
                    </div>
                </div>
                
                <!-- For background job progress -->
                <div class="job-progress-panel hidden">
                    <h3>Background Processing</h3>
                    <p>The batch is being processed in the background. You can check the status here or come back later.</p>
                    
                    <div class="progress-container">
                        <p><span class="job-id">-</span></p>
                        <p>Status: <span class="job-status">Queued</span></p>
                       
                    </div>
                    
                    
                </div>
            </div>
        `);
        console.log("Direct HTML content loaded");
        this.bind_events();
    }
    
    bind_events() {
        let me = this;
        console.log("Binding events...");
        
        // Batch selection change
        this.wrapper.find('.batch-selector').on('change', function() {
            let batch_id = $(this).val();
            if (batch_id) {
                me.load_batch_details(batch_id);
            } else {
                me.clear_batch_details();
            }
        });
        
        // Process button
        this.wrapper.find('.process-btn').on('click', function() {
            me.process_batch();
        });
        
        // Cancel button
        this.wrapper.find('.cancel-btn').on('click', function() {
            me.clear_batch_details();
            me.wrapper.find('.batch-selector').val('').trigger('change');
        });
        
        // Export buttons
        this.wrapper.find('.export-errors').on('click', function() {
            me.export_errors();
        });
        
        this.wrapper.find('.export-results').on('click', function() {
            me.export_results();
        });
        
        // Refresh background job status
        this.wrapper.find('.refresh-status').on('click', function() {
            me.check_job_status();
        });
        
        console.log("Events bound successfully");
    }
    
    load_data() {
        let me = this;
        
        // Load onboarding batches
        console.log("Loading batches...");
        frappe.call({
            method: 'tap_ai.tap_ai.page.backend_onboarding_process.backend_onboarding_process.get_onboarding_batches',
            callback: function(r) {
                if (r.message) {
                    let select = me.wrapper.find('.batch-selector');
                    select.empty();
                    select.append($('<option value="">').text(__('Select a Set')));
                    console.log("Batches received:", r.message);
                    $.each(r.message, function(i, batch) {
                        //let option_text = `${batch.set_name} (${batch.student_count} students)`;
                        let option_text = `${batch.set_name} `;
                        select.append($('<option>').val(batch.name).text(option_text));
                    });
                    console.log("Batches loaded:", r.message.length);
                } else {
                    console.log("No batches found or error loading batches");
                }
            },
            error: function(r) {
                console.error("Error loading batches:", r);
            }
        });
    }
    
    load_batch_details(batch_id) {
        let me = this;
        console.log("Loading batch details for:", batch_id);
        
        frappe.call({
            method: 'tap_ai.tap_ai.page.backend_onboarding_process.backend_onboarding_process.get_batch_details',
            args: {
                batch_id: batch_id
            },
            callback: function(r) {
                if (r.message) {
                    me.batch_data = r.message;
                    console.log("Batch details received:", r.message);
                    me.render_batch_summary(r.message);
                    //me.render_student_list(r.message.students);
                    if(r.message.on_queue){
                        me.wrapper.find('.process-btn').addClass('hidden');
                        me.wrapper.find('.cancel-btn').addClass('hidden');
                    }
                    me.wrapper.find('.action-panel').removeClass('hidden');
                    
                    // Hide results panel if visible from previous batch
                    me.wrapper.find('.results-panel, .job-progress-panel').addClass('hidden');
                    
                    console.log("Batch details loaded successfully");
                } else {
                    console.log("No batch details found or error loading details");
                }
            },
            error: function(r) {
                console.error("Error loading batch details:", r);
                frappe.msgprint(__("Error loading batch details. Please check the console for more information."));
            }
        });
    }
    
    render_batch_summary(batch) {
        let summary = this.wrapper.find('.batch-summary');
        summary.removeClass('hidden');
        
        // Status indicator
        let status_class = {
            'Draft': 'blue',
            'Processing': 'orange',
            'Processed': 'green',
            'Failed': 'red'
        };
        
        /* summary.find('.status-indicator').html(
            `<span class="indicator ${status_class[batch.status] || 'gray'}">
                ${__(batch.status)}
            </span>`
        ); */
        
        // Metrics
        let metrics = summary.find('.metrics');
        metrics.empty();
        
        metrics.append(`<div><strong>${__("Set Name")}:</strong> ${batch.batch}</div>`);
        metrics.append(`<div><strong>${__("Students")}:</strong> ${batch.students}</div>`);
        //metrics.append(`<div><strong>${__("Uploaded By")}:</strong> ${batch.uploaded_by}</div>`);
        //metrics.append(`<div><strong>${__("Upload Date")}:</strong> ${frappe.datetime.str_to_user(batch.upload_date)}</div>`);
        
        /* if (batch.processed_student_count) {
            metrics.append(`<div><strong>${__("Processed")}:</strong> ${batch.processed_student_count}/${batch.student_count}</div>`);
        } */
        
        console.log("Batch summary rendered");
    }
    
    render_student_list(students) {
        let table = this.wrapper.find('.student-preview');
        table.removeClass('hidden');
        
        let tbody = table.find('tbody');
        tbody.empty();
        
        table.find('.count').text(`(${students.length})`);
        
        $.each(students, function(i, student) {
            let validation_html = '';
            if (student.validation) {
                if (student.validation.duplicate) {
                    validation_html += `<span class="indicator orange">
                        ${__("Duplicate")} <a href="#Form/Student/${student.validation.duplicate.student_id}">
                            ${student.validation.duplicate.student_name}
                        </a>
                    </span>`;
                }
                
                for (let field in student.validation) {
                    if (field !== 'duplicate' && student.validation[field] === 'missing') {
                        validation_html += `<span class="indicator red">
                            ${__("Missing")} ${__(field)}
                        </span>`;
                    }
                }
            }
            
            let status_class = {
                'Pending': 'blue',
                'Success': 'green',
                'Failed': 'red'
            };
            
            let row = $(`<tr>
                <td><span class="indicator ${status_class[student.processing_status] || 'gray'}">${__(student.processing_status)}</span></td>
                <td>${student.student_name || ''}</td>
                <td>${student.phone || ''}</td>
                <td>${student.gender || ''}</td>
                <td>${student.school || ''}</td>
                <td>${student.grade || ''}</td>
                <td>${student.course_vertical || ''}</td>
                <td>${student.batch || ''}</td>
                <td>${student.language || ''}</td>
                <td class="validation-cell">${validation_html}</td>
            </tr>`);
            
            tbody.append(row);
        });
        
        console.log("Student list rendered with", students.length, "students");
    }
    
    process_batch() {
        let me = this;
        let batch_id = this.wrapper.find('.batch-selector').val();
        
        if (!batch_id) {
            frappe.msgprint(__("Please select a set first"));
            return;
        }
        
        // Get the value of background_job checkbox
        //let use_background_job = this.wrapper.find('input[name="background_job"]').prop('checked');
        let use_background_job = true; // Always use background job
        console.log("Processing batch, background job:", use_background_job);
        
        // Show confirmation dialog
        frappe.confirm(
            __(`Are you sure you want to process this batch? This will create student records and Glific contacts.`),
            function() {
                // On Yes
                if (use_background_job) {
                    me.start_background_job(batch_id);
                } else {
                    me.start_background_job(batch_id);
                    //me.start_foreground_job(batch_id);
                }
            }
        );
    }
    
    start_foreground_job(batch_id) {
        let me = this;
        me.show_processing_ui();
        
        frappe.call({
            method: 'tap_ai.tap_ai.page.backend_onboarding_process.backend_onboarding_process.process_batch',
            args: {
                batch_id: batch_id,
                use_background_job: false
            },
            callback: function(r) {
                if (r.message) {
                    me.show_results(r.message);
                    console.log("Batch processing completed", r.message);
                } else {
                    console.error("No response from process_batch or error occurred");
                    frappe.msgprint(__("Error processing batch. Please check the console for more information."));
                }
            },
            error: function(r) {
                console.error("Error in process_batch:", r);
                frappe.msgprint(__("Error processing batch. Please check the console for more information."));
            }
        });
    }
    
    start_background_job(batch_id) {
        let me = this;
        
        // Show job progress UI
        me.wrapper.find('.job-progress-panel').removeClass('hidden');
        me.wrapper.find('.job-status').text("Starting...");
        
        frappe.call({
            method: 'tap_ai.tap_ai.page.backend_onboarding_process.backend_onboarding_process.process_batch',
            args: {
                batch_id: batch_id,
                use_background_job: true
            },
            callback: function(r) {
                if (r.message && r.message.job_id) {
                    console.log("Background job started:", r.message);
                    me.job_id = r.message.job_id;
                    me.wrapper.find('.job-status').text("Queued");
                    me.wrapper.find('.job-processed').text(`0/${me.batch_data.students.length}`);
                    me.wrapper.find('.job-id').text(`Job ID: ${me.job_id}`);
                    // Set a timer to check job status periodically
                    me.job_check_timer = setInterval(function() {
                        me.check_job_status();
                    }, 5000); // Check every 5 seconds
                    
                    console.log("Background job started with ID:", me.job_id);
                } else {
                    console.error("No job ID received or error occurred");
                    frappe.msgprint(__("Error starting background job. Please check the console for more information."));
                }
            },
            error: function(r) {
                console.error("Error starting background job:", r);
                frappe.msgprint(__("Error starting background job. Please check the console for more information."));
            }
        });
    }
    
    check_job_status() {
        let me = this;
        if (!me.job_id) return;
        
        frappe.call({
            method: 'tap_ai.tap_ai.page.backend_onboarding_process.backend_onboarding_process.get_job_status',
            args: {
                job_id: me.job_id
            },
            callback: function(r) {
                if (r.message) {
                    let job_info = r.message;
                    me.wrapper.find('.job-status').text(job_info.status || "Unknown");
                    
                    // Handle Unknown status gracefully
                    if (job_info.status === "Unknown") {
                        clearInterval(me.job_check_timer);
                        // Show a message to the user
                        me.wrapper.find('.job-progress-panel').append(
                            `<div class="alert alert-warning">
                                Unable to determine job status. The process might still be running in the background.
                                <br>
                                <button class="btn btn-default btn-sm refresh-batch">Refresh Batch Details</button>
                            </div>`
                        );
                        
                        // Bind event for refreshing batch details
                        me.wrapper.find('.refresh-batch').on('click', function() {
                            me.load_batch_details(me.wrapper.find('.batch-selector').val());
                        });
                        return;
                    }
                    
                    if (job_info.progress) {
                        let percent = Math.round((job_info.progress.completed / job_info.progress.total) * 100);
                        me.wrapper.find('.job-progress-bar').css('width', percent + '%');
                        me.wrapper.find('.job-processed').text(`${job_info.progress.completed}/${job_info.progress.total}`);
                    }
                    
                    // If job is complete, show results
                    if (job_info.status === "Completed") {
                        clearInterval(me.job_check_timer);
                        me.load_batch_details(me.wrapper.find('.batch-selector').val()); // Refresh batch details
                        
                        if (job_info.result) {
                            me.show_results(job_info.result);
                        }
                        
                        me.wrapper.find('.job-progress-panel').addClass('hidden');
                    }
                    
                    // If job failed, show error
                    if (job_info.status === "Failed") {
                        clearInterval(me.job_check_timer);
                        frappe.msgprint(__("Background processing job failed. Please check the error logs."));
                    }
                }
            },
            error: function(r) {
                console.error("Error checking job status:", r);
                // If there's an error checking status, stop checking and show a message
                clearInterval(me.job_check_timer);
                me.wrapper.find('.job-progress-panel').append(
                    `<div class="alert alert-warning">
                        Error checking job status. The process might still be running in the background.
                        <br>
                        <button class="btn btn-default btn-sm refresh-batch">Refresh Batch Details</button>
                    </div>`
                );
                
                // Bind event for refreshing batch details
                me.wrapper.find('.refresh-batch').on('click', function() {
                    me.load_batch_details(me.wrapper.find('.batch-selector').val());
                });
            }
        });
    }
    
    show_processing_ui() {
        let results_panel = this.wrapper.find('.results-panel');
        results_panel.removeClass('hidden');
        
        results_panel.find('.progress-bar').css('width', '0%');
        results_panel.find('.processed-count').text('0/' + this.batch_data.students.length);
        results_panel.find('.success-count').text('0');
        results_panel.find('.failed-count').text('0');
        
        results_panel.find('.error-log, .success-log').addClass('hidden');
        
        console.log("Processing UI shown");
    }
    
    show_results(result) {
        let results_panel = this.wrapper.find('.results-panel');
        results_panel.removeClass('hidden');
        
        let total = result.success_count + result.failure_count;
        let percentage = Math.round((total / this.batch_data.students.length) * 100);
        
        results_panel.find('.progress-bar').css('width', percentage + '%');
        results_panel.find('.processed-count').text(total + '/' + this.batch_data.students.length);
        results_panel.find('.success-count').text(result.success_count);
        results_panel.find('.failed-count').text(result.failure_count);
        
        // Show error log if there are failures
        if (result.failure_count > 0) {
            let error_log = results_panel.find('.error-log');
            error_log.removeClass('hidden');
            
            let tbody = error_log.find('tbody');
            tbody.empty();
            
            $.each(result.results.failed, function(i, failure) {
                tbody.append($(`<tr>
                    <td>${failure.student_name}</td>
                    <td>${failure.error}</td>
                </tr>`));
            });
            
            console.log("Error log displayed with", result.failure_count, "failures");
        }
        
        // Show success log if there are successes
        if (result.success_count > 0) {
            let success_log = results_panel.find('.success-log');
            success_log.removeClass('hidden');
            
            let tbody = success_log.find('tbody');
            tbody.empty();
            
            $.each(result.results.success, function(i, success) {
                tbody.append($(`<tr>
                    <td>${success.student_name}</td>
                    <td>${success.phone}</td>
                    <td>${success.student_id}</td>
                    <td>${success.glific_id || 'N/A'}</td>
                </tr>`));
            });
            
            console.log("Success log displayed with", result.success_count, "successes");
        }
    }
    
    clear_batch_details() {
        this.wrapper.find('.batch-summary, .student-preview, .action-panel, .results-panel, .job-progress-panel').addClass('hidden');
        
        // Clear any job status check timer
        if (this.job_check_timer) {
            clearInterval(this.job_check_timer);
            this.job_check_timer = null;
        }
        
        console.log("Batch details cleared");
    }
    
    export_errors() {
        if (!this.batch_data) {
            console.log("No batch data available for export");
            return;
        }
        
        let data = [];
        $.each(this.batch_data.students, function(i, student) {
            if (student.processing_status === 'Failed') {
                data.push({
                    "Student Name": student.student_name,
                    "Phone": student.phone,
                    "School": student.school,
                    "Grade": student.grade,
                    "Course": student.course_vertical,
                    "Batch": student.batch,
                    "Language": student.language,
                    "Status": "Failed",
                    "Error": student.processing_notes || ""
                });
            }
        });
        
        this.export_csv(data, 'failed_students');
        console.log("Exported", data.length, "failed students");
    }
    
    export_results() {
        if (!this.batch_data) {
            console.log("No batch data available for export");
            return;
        }
        
        let data = [];
        $.each(this.batch_data.students, function(i, student) {
            data.push({
                "Student Name": student.student_name,
                "Phone": student.phone,
                "School": student.school,
                "Grade": student.grade,
                "Course": student.course_vertical,
                "Batch": student.batch,
                "Language": student.language,
                "Status": student.processing_status,
                "Student ID": student.student_id || '',
                "Glific ID": student.glific_id || ''
            });
        });
        
        this.export_csv(data, 'processed_students');
        console.log("Exported", data.length, "processed students");
    }
    
    export_csv(data, filename) {
        if (!data.length) {
            console.log("No data to export");
            return;
        }
        
        // Get headers
        let headers = Object.keys(data[0]);
        
        // Create CSV content
        let csv_content = headers.join(',') + '\n';
        
        // Add rows
        data.forEach(function(row) {
            let values = headers.map(function(header) {
                let value = row[header] || '';
                // Escape quotes and wrap in quotes if needed
                if (value && typeof value === 'string' && (value.includes(',') || value.includes('"'))) {
                    value = '"' + value.replace(/"/g, '""') + '"';
                }
                return value;
            });
            csv_content += values.join(',') + '\n';
        });
        
        // Create download link
        let blob = new Blob([csv_content], { type: 'text/csv;charset=utf-8;' });
        let link = document.createElement('a');
        link.href = URL.createObjectURL(blob);
        link.setAttribute('download', filename + '.csv');
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        
        console.log("CSV file", filename + '.csv', "created and downloaded");
    }
}
