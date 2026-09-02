/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.action.ActionRequest;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugins.ActionPlugin.ActionHandler;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Verifies the Stage 5 REST handlers and transport actions are registered by the plugin. */
public class MVApiRegistrationTests extends OpenSearchTestCase {

    public void testRestHandlersRegistered() {
        MVDataFormatPlugin plugin = new MVDataFormatPlugin();
        List<RestHandler> handlers = plugin.getRestHandlers(null, null, null, null, null, null, null);

        boolean hasValidate = handlers.stream().anyMatch(h -> h instanceof RestMVValidateAction);
        boolean hasView = handlers.stream().anyMatch(h -> h instanceof RestMVViewAction);
        assertTrue("POST /_mv/_validate handler must be registered", hasValidate);
        assertTrue("/_mv/views/{name} handler must be registered", hasView);
    }

    public void testValidateRoute() {
        List<RestHandler.Route> routes = new RestMVValidateAction().routes();
        assertEquals(1, routes.size());
        assertEquals(RestRequest.Method.POST, routes.get(0).getMethod());
        assertEquals("/_mv/_validate", routes.get(0).getPath());
    }

    public void testViewRoutes() {
        List<RestHandler.Route> routes = new RestMVViewAction().routes();
        assertEquals(3, routes.size());
        Set<RestRequest.Method> methods = new HashSet<>();
        for (RestHandler.Route r : routes) {
            assertEquals("/_mv/views/{name}", r.getPath());
            methods.add(r.getMethod());
        }
        assertTrue(methods.contains(RestRequest.Method.PUT));
        assertTrue(methods.contains(RestRequest.Method.GET));
        assertTrue(methods.contains(RestRequest.Method.DELETE));
    }

    public void testTransportActionsRegistered() {
        MVDataFormatPlugin plugin = new MVDataFormatPlugin();
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = plugin.getActions();
        Set<String> names = new HashSet<>();
        for (ActionHandler<? extends ActionRequest, ? extends ActionResponse> a : actions) {
            names.add(a.getAction().name());
        }
        assertTrue(names.contains(MVValidateAction.NAME));
        assertTrue(names.contains(MVCreateViewAction.NAME));
        assertTrue(names.contains(MVGetViewAction.NAME));
    }
}
