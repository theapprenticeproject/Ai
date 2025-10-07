frappe.pages['glific_id_update'].on_page_load = function(wrapper) {
    var page = frappe.ui.make_app_page({
        parent: wrapper,
        title: 'Update Student Glific IDs',
        single_column: true
    });

    page.set_primary_action('Start Update', function() {
        frappe.call({
            method: 'tap_ai.glific_utils.run_glific_id_update',
            callback: function(r) {
                frappe.show_alert(r.message);
            }
        });
    });

    // Add a progress bar
    $(wrapper).find('.layout-main-section').html('<div class="progress">' +
        '<div class="progress-bar" role="progressbar" style="width: 0%;" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100">0%</div>' +
    '</div>');

    // Listen for progress updates
    frappe.realtime.on('glific_id_update_progress', function(data) {
        var progress = (data.processed / data.total) * 100;
        $(wrapper).find('.progress-bar').css('width', progress + '%').attr('aria-valuenow', progress).text(Math.round(progress) + '%');
    });

    // Listen for completion
    frappe.realtime.on('glific_id_update_complete', function(data) {
        frappe.show_alert({message: `Update complete. Total updated: ${data.total_updated}`, indicator: 'green'});
    });
};
