import http from 'k6/http';
import { check } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('errors');

export const options = {
    stages: [
        { duration: '10s', target: 10 },
        { duration: '20s', target: 50 },
        { duration: '10s', target: 0 },
    ],

    thresholds: {
        http_req_duration: ['p(95)<500'],
        http_req_failed: ['rate<0.01'],
        errors: ['rate<0.05'],
    },
};

export default function () {
    const res = http.get('https://test.k6.io/');

    const success = check(res, {
        'status is 200': (r) => r.status === 200,
        'response has body': (r) => r.body.length > 0,
    });

    errorRate.add(!success);
}