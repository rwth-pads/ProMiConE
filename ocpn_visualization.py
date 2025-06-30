from dash import Output, Input, callback, no_update, State, dcc, html, clientside_callback
import pm4py
import base64
import json
import plotly.graph_objects as go
import difflib

import globals
import ocpn_json
import startup

clientside_callback(
    """
    function(href) {
        var w = window.innerWidth;
        var h = window.innerHeight;
        return {'height': h, 'width': w};
    }
    """,
    Output('screen-size', 'data'),
    Input('right-side', 'children')
)


@callback(Output('placeholder', 'children'),
          Input('screen-size', 'data'),
          prevent_initial_call=True)
def set_ocpn_size(screen_size):
    globals.ocpn_height = int(screen_size['height'] * 0.65)
    globals.ocpn_width = int(screen_size['width'] * 0.95)

    print(f"ocpn width calibrated at : {globals.ocpn_width}, ocpn height: {globals.ocpn_height}")
    return no_update


def generate_ocpn_image(ocpn):
    img_path = r'./ocpn.png'
    pm4py.save_vis_ocpn(ocpn=ocpn, file_path=img_path)

    with open(img_path, 'rb') as f:
        image = f.read()

    fig = go.Figure()
    img_width = globals.ocpn_width
    img_height = globals.ocpn_height
    scale_factor = 0.5

    # Add invisible scatter trace. This trace is added to help the autoresize logic work.
    fig.add_trace(
        go.Scatter(x=[0, img_width * scale_factor], y=[0, img_height * scale_factor], mode="markers", marker_opacity=0)
    )
    # Configure axes
    fig.update_xaxes(visible=False, range=[0, img_width * scale_factor])

    fig.update_yaxes(visible=False, range=[0, img_height * scale_factor],
                     # the scaleanchor attribute ensures that the aspect ratio stays constant
                     scaleanchor="x")
    # Add image
    fig.add_layout_image(dict(x=0, sizex=img_width * scale_factor, y=img_height * scale_factor,
                              sizey=img_height * scale_factor, xref="x", yref="y", opacity=1.0, layer="below",
                              source='data:image/png;base64,' + base64.b64encode(image).decode('utf-8')
                              ))
    # Configure other layout
    fig.update_layout(width=img_width * scale_factor, height=img_height * scale_factor,
                      margin={"l": 0, "r": 0, "t": 0, "b": 0})

    return fig


@callback(
    Output('div-provided-ocpn', 'children'),
    Output('div-upload', 'hidden'),
    Input('upload-data', 'contents'),
    Input('upload-data', 'filename'),
    prevent_initial_call=True)
def handle_upload(content, filename):
    if not content:
        return no_update
    if '.json' in filename:

        content_type, content_string = content.split(',')
        decoded = base64.b64decode(content_string).decode('utf-8')

        provided_ocpn = json.loads(decoded, cls=ocpn_json.OCPNDecoder)

        startup.update_df_tables(provided_ocpn)

        ocpn_div = dcc.Graph(id={'type': 'provided-ocpn', 'dummy': 0}, figure=generate_ocpn_image(provided_ocpn))
        globals.provided_ocpn = provided_ocpn

        return ocpn_div, True
    else:
        return no_update


@callback(Output({'type': 'modal', 'intend': 'ocpn'}, 'is_open'),
          Input('safe-ocpn', 'n_clicks'),
          prevent_initial_call=True)
def store_ocpn(n_clicks):
    if not n_clicks:
        return no_update

    fp = r'./ocpn.json'
    with open(fp, 'w') as f_ocpn:  # somehow json.dump() does not use the encoder...
        f_ocpn.write(json.dumps(obj=globals.extracted_ocpn, cls=ocpn_json.OCPNEncoder))  # indent=1,

    return True


@callback(Output('ocpn-difference', 'children'),
          Input('div-provided-ocpn', 'children'),
          Input('div-extracted-ocpn', 'children'),
          prevent_initial_call=True)
def get_ocpn_differences(div_provided_ocpn, div_extracted_ocpn):
    info_string = ''

    if globals.provided_ocpn and globals.extracted_ocpn:
        provided_event_types = list(globals.provided_ocpn['activities'])
        provided_object_types = list(globals.provided_ocpn['object_types'])
        extracted_event_types = list(globals.extracted_ocpn['activities'])
        extracted_object_types = list(globals.extracted_ocpn['object_types'])

        # seq.quick_ratio()
        ratio_event_types = [
            max(difflib.SequenceMatcher(lambda x: x in " -_,.", i, j).ratio() for j in extracted_event_types) for i in
            provided_event_types]
        # missing_event_types = [provided_ocpn[i] if r < 0.6 else None for i, r in enumerate(ratio_event_types)]
        missing_event_types = [provided_event_types[i] for i, r in enumerate(ratio_event_types) if r < 0.6]
        ratio_object_types = [
            max(difflib.SequenceMatcher(lambda x: x in " -_,.", i, j).ratio() for j in extracted_object_types) for i in
            provided_object_types]
        # missing_object_types = [provided_ocpn[i] if r < 0.6 else None for i, r in enumerate(ratio_object_types)]
        missing_object_types = [provided_object_types[i] for i, r in enumerate(ratio_object_types) if r < 0.6]

        if missing_event_types or missing_object_types:
            info_string = (f"Concepts present in the provided OCPN but not in the mined OCPN:\n"
                           f"Event types: "
                           f"{', '.join(missing_event_types) if missing_event_types else 'None'}\n"
                           f"Object types: "
                           f"{', '.join(missing_object_types) if missing_object_types else 'None'}")
    return info_string
