import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    scenarios: {
        browsing_users: {
            executor: 'ramping-vus',

            startVUs: 0,

            stages: [
                { duration: '10s', target: 20 },
                { duration: '30s', target: 50 },
                { duration: '10s', target: 0 },
            ],

            exec: 'browse',
        },

        api_load: {
            executor: 'constant-arrival-rate',

            rate: 20,
            timeUnit: '1s',

            duration: '50s',

            preAllocatedVUs: 10,
            maxVUs: 50,

            exec: 'apiRequests',
        },

        health_check: {
            executor: 'constant-vus',

            vus: 2,
            duration: '50s',

            exec: 'healthCheck',
        },
    },

    thresholds: {
        http_req_duration: ['p(95)<500'],
        http_req_failed: ['rate<0.01'],

        'http_req_duration{scenario:browsing_users}': [
            'p(95)<800',
        ],

        'http_req_duration{scenario:api_load}': [
            'p(95)<300',
        ],
    },
};


// Scenario 1
export function browse() {
    const res = http.get('https://test.k6.io/');

    check(res, {
        'homepage returns 200': (r) => r.status === 200,
    });

    sleep(1);
}


// Scenario 2
export function apiRequests() {
    const res = http.get('https://test.k6.io/api/myEndpoint');

    check(res, {
        'API returns 200': (r) => r.status === 200,
    });
}


// Scenario 3
export function healthCheck() {
    const res = http.get('https://test.k6.io/');

    check(res, {
        'service is healthy': (r) => r.status === 200,
    });
}